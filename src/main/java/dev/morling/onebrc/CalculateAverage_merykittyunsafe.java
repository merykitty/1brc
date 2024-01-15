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
import java.util.Map;
import java.util.TreeMap;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class CalculateAverage_merykittyunsafe {
    private static final String FILE = "./measurements.txt";
    private static final MemorySegment UNIVERSE = MemorySegment.NULL.reinterpret(Long.MAX_VALUE);

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED.length() >= 32
            ? ByteVector.SPECIES_256
            : ByteVector.SPECIES_128;
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
        // 100-byte key + 4-byte hash + 4-byte size +
        // 2-byte min + 2-byte max + 8-byte sum + 8-byte count
        private static final int ENTRY_SIZE = 128;
        private static final int SIZE_OFFSET = 0;
        private static final int MIN_OFFSET = 4;
        private static final int MAX_OFFSET = 6;
        private static final int SUM_OFFSET = 8;
        private static final int COUNT_OFFSET = 16;
        private static final int KEY_OFFSET = 24;

        // There is an assumption that map size <= 10000;
        private static final int CAPACITY = 1 << 17;
        private static final int ENTRY_MASK = ENTRY_SIZE * CAPACITY - 1;

        final long data;

        PoorManMap() {
            this.data = Arena.global().allocate(ENTRY_SIZE * CAPACITY, ENTRY_SIZE).address();
        }

        void observe(long entryOffset, long value) {
            long baseOffset = this.data + entryOffset;
            UNIVERSE.set(ValueLayout.JAVA_SHORT_UNALIGNED, baseOffset + MIN_OFFSET,
                    (short) Math.min(value, UNIVERSE.get(ValueLayout.JAVA_SHORT_UNALIGNED, baseOffset + MIN_OFFSET)));
            UNIVERSE.set(ValueLayout.JAVA_SHORT_UNALIGNED, baseOffset + MAX_OFFSET,
                    (short) Math.max(value, UNIVERSE.get(ValueLayout.JAVA_SHORT_UNALIGNED, baseOffset + MAX_OFFSET)));
            UNIVERSE.set(ValueLayout.JAVA_LONG_UNALIGNED, baseOffset + SUM_OFFSET,
                    value + UNIVERSE.get(ValueLayout.JAVA_LONG_UNALIGNED, baseOffset + SUM_OFFSET));
            UNIVERSE.set(ValueLayout.JAVA_LONG_UNALIGNED, baseOffset + COUNT_OFFSET,
                    1 + UNIVERSE.get(ValueLayout.JAVA_LONG_UNALIGNED, baseOffset + COUNT_OFFSET));
        }

        long indexSimple(long address, int size) {
            int x;
            int y;
            if (size >= Integer.BYTES) {
                x = UNIVERSE.get(ValueLayout.JAVA_INT_UNALIGNED, address);
                y = UNIVERSE.get(ValueLayout.JAVA_INT_UNALIGNED, address + size - Integer.BYTES);
            }
            else {
                x = UNIVERSE.get(ValueLayout.JAVA_BYTE, address);
                y = UNIVERSE.get(ValueLayout.JAVA_BYTE, address + size - Byte.BYTES);
            }
            int hash = hash(x, y);
            long entryOffset = (hash * ENTRY_SIZE) & ENTRY_MASK;
            for (;; entryOffset = (entryOffset + ENTRY_SIZE) & ENTRY_MASK) {
                int nodeSize = UNIVERSE.get(ValueLayout.JAVA_INT_UNALIGNED, this.data + entryOffset + SIZE_OFFSET);
                if (nodeSize == 0) {
                    insertInto(entryOffset, address, size);
                    return entryOffset;
                }
                else if (keyEqualScalar(entryOffset, address, size)) {
                    return entryOffset;
                }
            }
        }

        void insertInto(long entryOffset, long address, int size) {
            long baseOffset = this.data + entryOffset;
            UNIVERSE.set(ValueLayout.JAVA_INT_UNALIGNED, baseOffset + SIZE_OFFSET, size);
            UNIVERSE.set(ValueLayout.JAVA_LONG_UNALIGNED, baseOffset + MIN_OFFSET, Short.MAX_VALUE);
            UNIVERSE.set(ValueLayout.JAVA_SHORT_UNALIGNED, baseOffset + MAX_OFFSET, Short.MIN_VALUE);
            MemorySegment.copy(UNIVERSE, address, UNIVERSE, baseOffset + KEY_OFFSET, size + 1);
        }

        void mergeInto(Map<String, Aggregator> target) {
            for (int entryOffset = 0; entryOffset < CAPACITY * ENTRY_SIZE; entryOffset += ENTRY_SIZE) {
                long baseOffset = this.data + entryOffset;
                int size = UNIVERSE.get(ValueLayout.JAVA_INT_UNALIGNED, baseOffset + SIZE_OFFSET);
                if (size == 0) {
                    continue;
                }

                var data = UNIVERSE.asSlice(baseOffset + KEY_OFFSET, size).toArray(ValueLayout.JAVA_BYTE);
                String key = new String(data, StandardCharsets.UTF_8);
                target.compute(key, (k, v) -> {
                    if (v == null) {
                        v = new Aggregator();
                    }

                    v.min = Math.min(v.min, UNIVERSE.get(ValueLayout.JAVA_SHORT_UNALIGNED, baseOffset + MIN_OFFSET));
                    v.max = Math.max(v.max, UNIVERSE.get(ValueLayout.JAVA_SHORT_UNALIGNED, baseOffset + MAX_OFFSET));
                    v.sum += UNIVERSE.get(ValueLayout.JAVA_LONG_UNALIGNED, baseOffset + SUM_OFFSET);
                    v.count += UNIVERSE.get(ValueLayout.JAVA_LONG_UNALIGNED, baseOffset + COUNT_OFFSET);
                    return v;
                });
            }
        }

        static int hash(int x, int y) {
            int seed = 0x9E3779B9;
            int rotate = 5;
            return (Integer.rotateLeft(x * seed, rotate) ^ y) * seed; // FxHash
        }

        private boolean keyEqualScalar(long entryOffset, long address, int size) {
            long baseOffset = this.data + entryOffset;
            if (UNIVERSE.get(ValueLayout.JAVA_INT_UNALIGNED, baseOffset + SIZE_OFFSET) != size) {
                return false;
            }

            // Be simple
            for (long i = 0; i < size; i++) {
                int c1 = UNIVERSE.get(ValueLayout.JAVA_BYTE, baseOffset + KEY_OFFSET + i);
                int c2 = UNIVERSE.get(ValueLayout.JAVA_BYTE, address + i);
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
    private static long parseDataPoint(PoorManMap aggrMap, long entryOffset, long address) {
        long word = UNIVERSE.get(ValueLayout.JAVA_LONG_UNALIGNED, address);
        if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
            word = Long.reverseBytes(word);
        }
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
        aggrMap.observe(entryOffset, value);
        return address + (decimalSepPos >>> 3) + 3;
    }

    // Tail processing version of the above, do not over-fetch and be simple
    private static long parseDataPointSimple(PoorManMap aggrMap, long entryOffset, long address) {
        int value = 0;
        boolean negative = false;
        if (UNIVERSE.get(ValueLayout.JAVA_BYTE, address) == '-') {
            negative = true;
            address++;
        }
        for (;; address++) {
            int c = UNIVERSE.get(ValueLayout.JAVA_BYTE, address);
            if (c == '.') {
                c = UNIVERSE.get(ValueLayout.JAVA_BYTE, address + 1);
                value = value * 10 + (c - '0');
                address += 3;
                break;
            }

            value = value * 10 + (c - '0');
        }
        value = negative ? -value : value;
        aggrMap.observe(entryOffset, value);
        return address;
    }

    // An iteration of the main parse loop, parse a line starting from offset.
    // This requires offset to be the start of the line and there is spare space so
    // that we have relative freedom in processing
    // It returns the offset of the next line that it needs processing
    private static long iterate(PoorManMap aggrMap, long address) {
        ByteVector line = ByteVector.fromMemorySegment(BYTE_SPECIES, UNIVERSE, address, ByteOrder.nativeOrder());

        // Find the delimiter ';'
        long semicolons = line.compare(VectorOperators.EQ, ';').toLong();

        // If we cannot find the delimiter in the vector, that means the key is
        // longer than the vector, fall back to scalar processing
        if (semicolons == 0) {
            int keySize = BYTE_SPECIES.length();
            while (UNIVERSE.get(ValueLayout.JAVA_BYTE, address + keySize) != ';') {
                keySize++;
            }
            var node = aggrMap.indexSimple(address, keySize);
            return parseDataPoint(aggrMap, node, address + 1 + keySize);
        }

        // We inline the searching of the value in the hash map
        int keySize = Long.numberOfTrailingZeros(semicolons);
        int x;
        int y;
        if (keySize >= Integer.BYTES) {
            x = UNIVERSE.get(ValueLayout.JAVA_INT_UNALIGNED, address);
            y = UNIVERSE.get(ValueLayout.JAVA_INT_UNALIGNED, address + keySize - Integer.BYTES);
        }
        else {
            x = UNIVERSE.get(ValueLayout.JAVA_BYTE, address);
            y = UNIVERSE.get(ValueLayout.JAVA_BYTE, address + keySize - Byte.BYTES);
        }
        int hash = PoorManMap.hash(x, y);
        long entryOffset = (hash * PoorManMap.ENTRY_SIZE) & PoorManMap.ENTRY_MASK;
        for (;; entryOffset = (entryOffset + PoorManMap.ENTRY_SIZE) & PoorManMap.ENTRY_MASK) {
            long baseOffset = entryOffset + aggrMap.data;
            var nodeSize = UNIVERSE.get(ValueLayout.JAVA_INT_UNALIGNED, baseOffset + PoorManMap.SIZE_OFFSET);
            if (nodeSize == 0) {
                aggrMap.insertInto(entryOffset, address, keySize);
                break;
            }

            if (nodeSize != keySize) {
                continue;
            }

            var nodeKey = ByteVector.fromMemorySegment(BYTE_SPECIES, UNIVERSE, baseOffset + PoorManMap.KEY_OFFSET, ByteOrder.nativeOrder());
            long eqMask = line.compare(VectorOperators.EQ, nodeKey).toLong();
            long validMask = semicolons ^ (semicolons - 1);
            if ((eqMask & validMask) == validMask) {
                break;
            }
        }

        return parseDataPoint(aggrMap, entryOffset, address + keySize + 1);
    }

    // Process all lines that start in [offset, limit)
    private static PoorManMap processFile(MemorySegment data, long offset, long limit) {
        var aggrMap = new PoorManMap();
        long base = data.address();
        long begin = base + offset;
        long end = base + limit;
        // Find the start of a new line
        if (offset != 0) {
            begin--;
            while (begin < end) {
                if (UNIVERSE.get(ValueLayout.JAVA_BYTE, begin++) == '\n') {
                    break;
                }
            }
        }

        // If there is no line starting in this segment, just return
        if (begin == end) {
            return aggrMap;
        }

        // The main loop, optimized for speed
        while (begin < end - Math.max(BYTE_SPECIES.vectorByteSize(),
                Long.BYTES + 1 + KEY_MAX_SIZE)) {
            begin = iterate(aggrMap, begin);
        }

        // Now we are at the tail, just be simple
        while (begin < end) {
            int keySize = 0;
            while (UNIVERSE.get(ValueLayout.JAVA_BYTE, begin + keySize) != ';') {
                keySize++;
            }
            long entryOffset = aggrMap.indexSimple(begin, keySize);
            begin = parseDataPointSimple(aggrMap, entryOffset, begin + 1 + keySize);
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
                aggrMap.mergeInto(res);
            }
        }

        System.out.println(res);
    }
}
