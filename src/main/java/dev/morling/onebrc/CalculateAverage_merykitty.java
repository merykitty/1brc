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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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

public class CalculateAverage_merykitty {
    private static final String FILE = "./measurements.txt";
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED.length() >= 32
            ? ByteVector.SPECIES_256
            : ByteVector.SPECIES_128;
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

        // 100-byte key + 4-byte hash + 4-byte size +
        // 2-byte min + 2-byte max + 8-byte sum + 8-byte count
        private static final int ENTRY_SIZE = 128;
        private static final int MIN_OFFSET = 0;
        private static final int MAX_OFFSET = 4;

        private static final int SUM_OFFSET = 8;
        private static final int COUNT_OFFSET = 16;
        private static final int SIZE_OFFSET = 24;
        private static final int KEY_OFFSET = 28;

        // There is an assumption that map size <= 10000;
        private static final int CAPACITY = 1 << 17;
        private static final int ENTRY_OFFSET_MASK = (CAPACITY * ENTRY_SIZE) - 1;

        private static final VarHandle LONG_ACCESSOR = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder()).withInvokeExactBehavior();
        private static final VarHandle INT_ACCESSOR = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder()).withInvokeExactBehavior();

        byte[] data;

        PoorManMap() {
            this.data = new byte[CAPACITY * ENTRY_SIZE];
        }

        int size(int entryOffset) {
            return (int) INT_ACCESSOR.get(this.data, entryOffset + SIZE_OFFSET);
        }

        void observe(int entryOffset, long value) {
            INT_ACCESSOR.set(this.data, entryOffset + MIN_OFFSET,
                    (int) Math.min(value, (int) INT_ACCESSOR.get(this.data, entryOffset + MIN_OFFSET)));
            INT_ACCESSOR.set(this.data, entryOffset + MAX_OFFSET,
                    (int) Math.max(value, (int) INT_ACCESSOR.get(this.data, entryOffset + MAX_OFFSET)));
            LONG_ACCESSOR.set(this.data, entryOffset + SUM_OFFSET,
                    value + (long) LONG_ACCESSOR.get(this.data, entryOffset + SUM_OFFSET));
            LONG_ACCESSOR.set(this.data, entryOffset + COUNT_OFFSET,
                    1 + (long) LONG_ACCESSOR.get(this.data, entryOffset + COUNT_OFFSET));
        }

        int indexSimple(MemorySegment data, long offset, int size) {
            int x;
            int y;
            if (size >= Integer.BYTES) {
                x = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
                y = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset + size - Integer.BYTES);
            }
            else {
                x = data.get(ValueLayout.JAVA_BYTE, offset);
                y = data.get(ValueLayout.JAVA_BYTE, offset + size - Byte.BYTES);
            }
            int hash = hash(x, y);
            int entryOffset = (hash * ENTRY_SIZE) & PoorManMap.ENTRY_OFFSET_MASK;
            for (;; entryOffset = (entryOffset + ENTRY_SIZE) & PoorManMap.ENTRY_OFFSET_MASK) {
                int nodeSize = size(entryOffset);
                if (nodeSize == 0) {
                    insertInto(entryOffset, data, offset, size);
                    return entryOffset;
                }
                else if (keyEqualScalar(entryOffset, data, offset, size)) {
                    return entryOffset;
                }
            }
        }

        void insertInto(int entryOffset, MemorySegment data, long offset, int size) {
            INT_ACCESSOR.set(this.data, entryOffset + SIZE_OFFSET, size);
            INT_ACCESSOR.set(this.data, entryOffset + MIN_OFFSET, Integer.MAX_VALUE);
            INT_ACCESSOR.set(this.data, entryOffset + MAX_OFFSET, Integer.MIN_VALUE);
            MemorySegment.copy(data, offset, MemorySegment.ofArray(this.data), entryOffset + KEY_OFFSET, size);
        }

        void mergeInto(Map<String, Aggregator> target) {
            for (int i = 0; i < CAPACITY; i++) {
                int entryOffset = i * ENTRY_SIZE;
                int size = size(entryOffset);
                if (size == 0) {
                    continue;
                }

                long min = (int) INT_ACCESSOR.get(this.data, entryOffset + MIN_OFFSET);
                long max = (int) INT_ACCESSOR.get(this.data, entryOffset + MAX_OFFSET);
                long sum = (long) LONG_ACCESSOR.get(this.data, entryOffset + SUM_OFFSET);
                long count = (long) LONG_ACCESSOR.get(this.data, entryOffset + COUNT_OFFSET);
                String key = new String(this.data, entryOffset + KEY_OFFSET, size, StandardCharsets.UTF_8);
                target.compute(key, (k, v) -> {
                    if (v == null) {
                        v = new Aggregator();
                    }

                    v.min = Math.min(v.min, min);
                    v.max = Math.max(v.max, max);
                    v.sum += sum;
                    v.count += count;
                    return v;
                });
            }
        }

        static int hash(int x, int y) {
            int seed = 0x9E3779B9;
            int rotate = 5;
            return (Integer.rotateLeft(x * seed, rotate) ^ y) * seed; // FxHash
        }

        private boolean keyEqualScalar(int entryOffset, MemorySegment data, long offset, int size) {
            if (size(entryOffset) != size) {
                return false;
            }

            // Be simple
            for (int i = 0; i < size; i++) {
                int c1 = this.data[entryOffset + KEY_OFFSET + i];
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
    private static long parseDataPoint(PoorManMap aggrMap, int entryOffset, MemorySegment data, long offset) {
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
        aggrMap.observe(entryOffset, value);
        return offset + (decimalSepPos >>> 3) + 3;
    }

    // Tail processing version of the above, do not over-fetch and be simple
    private static long parseDataPointSimple(PoorManMap aggrMap, int entryOffset, MemorySegment data, long offset) {
        int value = 0;
        boolean negative = false;
        if (data.get(ValueLayout.JAVA_BYTE, offset) == '-') {
            negative = true;
            offset++;
        }
        for (;; offset++) {
            int c = data.get(ValueLayout.JAVA_BYTE, offset);
            if (c == '.') {
                c = data.get(ValueLayout.JAVA_BYTE, offset + 1);
                value = value * 10 + (c - '0');
                offset += 3;
                break;
            }

            value = value * 10 + (c - '0');
        }
        value = negative ? -value : value;
        aggrMap.observe(entryOffset, value);
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
            while (data.get(ValueLayout.JAVA_BYTE, offset + keySize) != ';') {
                keySize++;
            }
            int bucket = aggrMap.indexSimple(data, offset, keySize);
            return parseDataPoint(aggrMap, bucket, data, offset + 1 + keySize);
        }

        // We inline the searching of the value in the hash map
        int x;
        int y;
        if (keySize >= Integer.BYTES) {
            x = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
            y = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset + keySize - Integer.BYTES);
        }
        else {
            x = data.get(ValueLayout.JAVA_BYTE, offset);
            y = data.get(ValueLayout.JAVA_BYTE, offset + keySize - Byte.BYTES);
        }
        int hash = PoorManMap.hash(x, y);
        int entryOffset = (hash * PoorManMap.ENTRY_SIZE) & PoorManMap.ENTRY_OFFSET_MASK;
        for (;; entryOffset = (entryOffset + PoorManMap.ENTRY_SIZE) & PoorManMap.ENTRY_OFFSET_MASK) {
            int nodeSize = aggrMap.size(entryOffset);
            if (nodeSize != keySize) {
                if (nodeSize != 0) {
                    continue;
                }
                aggrMap.insertInto(entryOffset, data, offset, keySize);
                break;
            }

            var nodeKey = ByteVector.fromArray(BYTE_SPECIES, aggrMap.data, entryOffset + PoorManMap.KEY_OFFSET);
            long eqMask = line.compare(VectorOperators.EQ, nodeKey).toLong();
            long validMask = -1L >>> -keySize;
            if ((eqMask & validMask) == validMask) {
                break;
            }
        }

        return parseDataPoint(aggrMap, entryOffset, data, offset + keySize + 1);
    }

    // Process all lines that start in [offset, limit)
    private static PoorManMap processFile(MemorySegment data, long offset, long limit) {
        var aggrMap = new PoorManMap();
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
            while (data.get(ValueLayout.JAVA_BYTE, offset + keySize) != ';') {
                keySize++;
            }
            int bucket = aggrMap.indexSimple(data, offset, keySize);
            offset = parseDataPointSimple(aggrMap, bucket, data, offset + 1 + keySize);
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
