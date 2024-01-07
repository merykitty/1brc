/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.TreeMap;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class CalculateAverage_merykitty {
    private static final String FILE = "./measurements.txt";
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final ValueLayout.OfLong JAVA_LONG_LT = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final long KEY_MAX_SIZE = 100;

    private static class Aggregator {
        private long min = Integer.MAX_VALUE;
        private long max = Integer.MIN_VALUE;
        private long sum;
        private long count;

        public String toString() {
            return round(min / 10.) + "/" + round(sum / (double) (10 * count)) + "/" + round(max / 10.);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    // An open-address map that is specialized for this task
    private static class PoorManMap {
        static final int R_LOAD_FACTOR = 2;

        private static class PoorManMapNode {
            byte[] data;
            int size;
            int hash;
            Aggregator aggr;

            PoorManMapNode(MemorySegment data, long offset, int size, int hash) {
                this.hash = hash;
                this.size = size;
                this.data = new byte[Math.max(size, BYTE_SPECIES.vectorByteSize())];
                this.aggr = new Aggregator();
                MemorySegment.copy(data, offset, MemorySegment.ofArray(this.data), 0, size);
            }
        }

        MemorySegment data;
        PoorManMapNode[] nodes;
        int size;

        PoorManMap(MemorySegment data) {
            this.data = data;
            this.nodes = new PoorManMapNode[1 << 10];
        }

        Aggregator indexSimple(long offset, int size, int hash) {
            hash = rehash(hash);
            int bucketMask = nodes.length - 1;
            int bucket = hash & bucketMask;
            for (;; bucket = (bucket + 1) & bucketMask) {
                PoorManMapNode node = nodes[bucket];
                if (node == null) {
                    this.size++;
                    if (this.size * R_LOAD_FACTOR > nodes.length) {
                        grow();
                        bucketMask = nodes.length - 1;
                        for (bucket = hash & bucketMask; nodes[bucket] != null; bucket = (bucket + 1) & bucketMask) {
                        }
                    }
                    node = new PoorManMapNode(this.data, offset, size, hash);
                    nodes[bucket] = node;
                    return node.aggr;
                }
                else if (keyEqualScalar(node, offset, size, hash)) {
                    return node.aggr;
                }
            }
        }

        void grow() {
            var oldNodes = this.nodes;
            var newNodes = new PoorManMapNode[oldNodes.length * 2];
            int bucketMask = newNodes.length - 1;
            for (var node : oldNodes) {
                if (node == null) {
                    continue;
                }
                int bucket = node.hash & bucketMask;
                for (; newNodes[bucket] != null; bucket = (bucket + 1) & bucketMask) {
                }
                newNodes[bucket] = node;
            }
            this.nodes = newNodes;
        }

        static int rehash(int x) {
            x = ((x >>> 16) ^ x) * 0x45d9f3b;
            x = ((x >>> 16) ^ x) * 0x45d9f3b;
            x = (x >>> 16) ^ x;
            return x;
        }

        private boolean keyEqualScalar(PoorManMapNode node, long offset, long size, int hash) {
            if (node.hash != hash || node.size != size) {
                return false;
            }

            // Be simple
            for (int i = 0; i < size; i++) {
                int c1 = node.data[i];
                int c2 = data.get(ValueLayout.JAVA_BYTE, offset + i);
                if (c1 != c2) {
                    return false;
                }
            }
            return true;
        }
    }

    // Parse a number that may/may not contain a minus sign followed by a decimal with
    // 1 - 2 digits to the left and 1 digits to the right of the separator to a
    // fix-precision format. It returns the offset of the next line (presumably followed
    // the final digit and a '\n')
    private static long parseDataPoint(Aggregator aggr, MemorySegment data, long offset) {
        long word = data.get(JAVA_LONG_LT, offset);
        // The 4th binary digit of the ascii of a digit is 1 while
        // that of the '.' is 0. This finds the decimal separator
        // The value can be 12, 20, 28
        int decimalSepPos = Long.numberOfTrailingZeros(~word & 0x10101000);
        int shift = 28 - decimalSepPos;
        // signed is -1 if negative, 0 otherwise
        long signed = (~word << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        // Align the number to a specific position and transform the ascii code
        // to actual digit value in each byte
        long digits = ((word & designMask) << shift) & 0x0F000F0F00L;

        // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
        // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
        // 0x000000UU00TTHH00 +
        // 0x00UU00TTHH000000 * 10 +
        // 0xUU00TTHH00000000 * 100
        // Now TT * 100 has 2 trailing zeroes and HH * 100 + TT * 10 + UU < 0x400
        // This results in our value lies in the bit 32 to 41 of this product
        // That was close :)
        long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
        long value = (absValue ^ signed) - signed;
        aggr.min = Math.min(value, aggr.min);
        aggr.max = Math.max(value, aggr.max);
        aggr.sum += value;
        aggr.count++;
        return offset + (decimalSepPos >>> 3) + 3;
    }

    // Tail processing version of the above, do not over-fetch and be simple
    private static long parseDataPointTail(Aggregator aggr, MemorySegment data, long offset) {
        int point = 0;
        boolean negative = false;
        if (data.get(ValueLayout.JAVA_BYTE, offset) == '-') {
            negative = true;
            offset++;
        }
        for (;; offset++) {
            int c = data.get(ValueLayout.JAVA_BYTE, offset);
            if (c == '.') {
                c = data.get(ValueLayout.JAVA_BYTE, offset + 1);
                point = point * 10 + (c - '0');
                offset += 3;
                break;
            }

            point = point * 10 + (c - '0');
        }
        point = negative ? -point : point;
        aggr.min = Math.min(point, aggr.min);
        aggr.max = Math.max(point, aggr.max);
        aggr.sum += point;
        aggr.count++;
        return offset;
    }

    // An iteration of the main parse loop, parse a line starting from offset.
    // This requires offset to be the start of the line and there is spare space so
    // that we have relative freedom in processing
    // It returns the offset of the next line that it needs processing
    private static long iterate(PoorManMap aggrMap, MemorySegment data, long offset) {
        var line = ByteVector.fromMemorySegment(BYTE_SPECIES, data, offset, ByteOrder.nativeOrder());

        // Find the delimiter ';'
        int keySize = line.compare(VectorOperators.EQ, ';').firstTrue();

        // If we cannot find the delimiter in the vector, that means the key is
        // longer than the vector, fall back to scalar processing
        if (keySize == BYTE_SPECIES.vectorByteSize()) {
            for (; data.get(ValueLayout.JAVA_BYTE, offset + keySize) != ';'; keySize++) {
            }
            int hash = line.reinterpretAsInts().lane(0);
            var aggr = aggrMap.indexSimple(offset, keySize, hash);
            return parseDataPoint(aggr, data, offset + 1 + keySize);
        }

        int hash = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        if (keySize < Integer.BYTES) {
            hash = (byte) hash;
        }

        // We inline the searching of the value in the hash map
        Aggregator aggr;
        hash = PoorManMap.rehash(hash);
        int bucketMask = aggrMap.nodes.length - 1;
        int bucket = hash & bucketMask;
        for (;; bucket = (bucket + 1) & bucketMask) {
            PoorManMap.PoorManMapNode node = aggrMap.nodes[bucket];
            if (node == null) {
                aggrMap.size++;
                if (aggrMap.size * PoorManMap.R_LOAD_FACTOR > aggrMap.nodes.length) {
                    aggrMap.grow();
                    bucketMask = aggrMap.nodes.length - 1;
                    for (bucket = hash & bucketMask; aggrMap.nodes[bucket] != null; bucket = (bucket + 1) & bucketMask) {
                    }
                }
                node = new PoorManMap.PoorManMapNode(data, offset, keySize, hash);
                aggrMap.nodes[bucket] = node;
                aggr = node.aggr;
                break;
            }

            if (node.hash != hash || node.size != keySize) {
                continue;
            }

            var nodeKey = ByteVector.fromArray(BYTE_SPECIES, node.data, 0);
            long eqMask = line.compare(VectorOperators.EQ, nodeKey).toLong();
            long validMask = -1L >>> -keySize;
            if ((eqMask & validMask) == validMask) {
                aggr = node.aggr;
                break;
            }
        }

        return parseDataPoint(aggr, data, offset + 1 + keySize);
    }

    // Process all lines that start in [offset, limit)
    private static PoorManMap processFile(MemorySegment data, long offset, long limit) {
        var aggrMap = new PoorManMap(data);
        // Find the start of a new line
        if (offset != 0) {
            offset--;
            while (offset < limit) {
                if (data.get(ValueLayout.JAVA_BYTE, offset++) == '\n') {
                    break;
                }
            }
        }

        // If there is no line starting in this segment, just return
        if (offset == limit) {
            return aggrMap;
        }

        // The main loop, optimized for speed
        while (offset < limit - Math.max(BYTE_SPECIES.vectorByteSize(),
                Long.BYTES + 1 + KEY_MAX_SIZE)) {
            offset = iterate(aggrMap, data, offset);
        }

        // Now we are at the tail, just be simple
        while (offset < limit) {
            int keySize = 0;
            for (; data.get(ValueLayout.JAVA_BYTE, offset + keySize) != ';'; keySize++) {
            }
            int hash;
            if (keySize >= Integer.BYTES) {
                hash = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
            }
            else {
                hash = data.get(ValueLayout.JAVA_BYTE, offset);
            }
            var aggr = aggrMap.indexSimple(offset, keySize, hash);
            offset = parseDataPointTail(aggr, data, offset + 1 + keySize);
        }

        return aggrMap;
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        int processorCnt = Runtime.getRuntime().availableProcessors();
        var res = new TreeMap<String, Aggregator>();
        try (var file = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);
                var arena = Arena.ofShared()) {
            var data = file.map(MapMode.READ_ONLY, 0, file.size(), arena);
            long chunkSize = Math.ceilDiv(data.byteSize(), processorCnt);
            var threadList = new Thread[processorCnt];
            var resultList = new PoorManMap[processorCnt];
            for (int i = 0; i < processorCnt; i++) {
                int index = i;
                long offset = i * chunkSize;
                long limit = Math.min((i + 1) * chunkSize, data.byteSize());
                var thread = new Thread(() -> resultList[index] = processFile(data, offset, limit));
                threadList[index] = thread;
                thread.start();
            }
            for (var thread : threadList) {
                thread.join();
            }

            // Collect the results
            for (var aggrMap : resultList) {
                for (var node : aggrMap.nodes) {
                    if (node == null) {
                        continue;
                    }
                    byte[] keyData = Arrays.copyOfRange(node.data, 0, node.size);
                    String key = new String(keyData, StandardCharsets.UTF_8);
                    var aggr = node.aggr;
                    var resAggr = new Aggregator();
                    var existingAggr = res.putIfAbsent(key, resAggr);
                    if (existingAggr != null) {
                        resAggr = existingAggr;
                    }
                    resAggr.min = Math.min(resAggr.min, aggr.min);
                    resAggr.max = Math.max(resAggr.max, aggr.max);
                    resAggr.sum += aggr.sum;
                    resAggr.count += aggr.count;
                }
            }
        }

        System.out.println(res);
    }
}
