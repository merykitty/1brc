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
        private static final int R_LOAD_FACTOR = 2;

        // 100-byte key + 4-byte hash + 4-byte size +
        // 2-byte min + 2-byte max + 8-byte sum + 8-byte count
        private static final int ENTRY_SIZE = 128;
        private static final int SIZE_OFFSET = 0;
        private static final int HASH_OFFSET = 4;
        private static final int SUM_OFFSET = 8;
        private static final int COUNT_OFFSET = 16;
        private static final int MIN_OFFSET = 24;
        private static final int MAX_OFFSET = 26;
        private static final int KEY_OFFSET = 28;
        private static final int DEFAULT_CAPACITY = 1 << 13;

        private static final VarHandle LONG_ACCESSOR = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder()).withInvokeExactBehavior();
        private static final VarHandle INT_ACCESSOR = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder()).withInvokeExactBehavior();
        private static final VarHandle SHORT_ACCESSOR = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.nativeOrder()).withInvokeExactBehavior();

        byte[] data;

        int size;
        int capacity;

        PoorManMap() {
            this.capacity = DEFAULT_CAPACITY;
            this.data = new byte[DEFAULT_CAPACITY * ENTRY_SIZE];
        }

        int size(int idx) {
            return (int) INT_ACCESSOR.get(this.data, idx * ENTRY_SIZE + SIZE_OFFSET);
        }

        int keyOffset(int idx) {
            return idx * ENTRY_SIZE + KEY_OFFSET;
        }

        void observe(int idx, long value) {
            int offset = idx * ENTRY_SIZE;
            SHORT_ACCESSOR.set(this.data, offset + MIN_OFFSET,
                    (short) Math.min(value, (short) SHORT_ACCESSOR.get(this.data, offset + MIN_OFFSET)));
            SHORT_ACCESSOR.set(this.data, offset + MAX_OFFSET,
                    (short) Math.max(value, (short) SHORT_ACCESSOR.get(this.data, offset + MAX_OFFSET)));
            LONG_ACCESSOR.set(this.data, offset + SUM_OFFSET,
                    value + (long) LONG_ACCESSOR.get(this.data, offset + SUM_OFFSET));
            LONG_ACCESSOR.set(this.data, offset + COUNT_OFFSET,
                    1 + (long) LONG_ACCESSOR.get(this.data, offset + COUNT_OFFSET));
        }

        int indexSimple(MemorySegment data, long offset, int size) {
            int hash;
            if (size >= Integer.BYTES) {
                hash = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset)
                        + data.get(ValueLayout.JAVA_INT_UNALIGNED, offset + size - Integer.BYTES);
            }
            else {
                hash = data.get(ValueLayout.JAVA_BYTE, offset);
            }
            hash = rehash(hash);
            int bucketMask = this.capacity - 1;
            int bucket = hash & bucketMask;
            for (;; bucket = (bucket + 1) & bucketMask) {
                int nodeSize = size(bucket);
                if (nodeSize == 0) {
                    return insertInto(bucket, data, offset, size, hash);
                }
                else if (keyEqualScalar(bucket, data, offset, size)) {
                    return bucket;
                }
            }
        }

        int insertInto(int bucket, MemorySegment data, long offset, int size, int hash) {
            this.size++;
            if (this.size * R_LOAD_FACTOR > this.capacity) {
                grow();
                int bucketMask = this.capacity - 1;
                for (bucket = hash & bucketMask; size(bucket) != 0; bucket = (bucket + 1) & bucketMask) {
                }
            }
            int entryOffset = bucket * ENTRY_SIZE;
            INT_ACCESSOR.set(this.data, entryOffset + SIZE_OFFSET, size);
            INT_ACCESSOR.set(this.data, entryOffset + HASH_OFFSET, hash);
            SHORT_ACCESSOR.set(this.data, entryOffset + MIN_OFFSET, Short.MAX_VALUE);
            SHORT_ACCESSOR.set(this.data, entryOffset + MAX_OFFSET, Short.MIN_VALUE);
            MemorySegment.copy(data, offset, MemorySegment.ofArray(this.data), entryOffset + KEY_OFFSET, size);
            return bucket;
        }

        void mergeInto(Map<String, Aggregator> target) {
            for (int i = 0; i < this.capacity; i++) {
                int size = size(i);
                if (size == 0) {
                    continue;
                }

                int entryOffset = i * ENTRY_SIZE;
                long min = (short) SHORT_ACCESSOR.get(this.data, entryOffset + MIN_OFFSET);
                long max = (short) SHORT_ACCESSOR.get(this.data, entryOffset + MAX_OFFSET);
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

        static int rehash(int x) {
            x = ((x >>> 16) ^ x) * 0x45d9f3b;
            x = ((x >>> 16) ^ x) * 0x45d9f3b;
            x = (x >>> 16) ^ x;
            return x;
        }

        private void grow() {
            int oldCapacity = this.capacity;
            var oldData = this.data;
            var newCapacity = oldCapacity * 2;
            var newData = new byte[newCapacity * ENTRY_SIZE];
            int bucketMask = newCapacity - 1;
            for (int i = 0; i < oldCapacity; i++) {
                int oldOffset = i * ENTRY_SIZE;
                int size = (int) INT_ACCESSOR.get(oldData, oldOffset + SIZE_OFFSET);
                if (size == 0) {
                    continue;
                }
                int bucket = (int) INT_ACCESSOR.get(oldData, oldOffset + HASH_OFFSET) & bucketMask;
                while ((int) INT_ACCESSOR.get(newData, bucket * ENTRY_SIZE + SIZE_OFFSET) != 0) {
                    bucket = (bucket + 1) & bucketMask;
                }
                for (int j = 0; j < KEY_OFFSET + size; j++) {
                    newData[bucket * ENTRY_SIZE + j] = oldData[oldOffset + j];
                }
            }
            this.data = newData;
            this.capacity = newCapacity;
        }

        private boolean keyEqualScalar(int bucket, MemorySegment data, long offset, int size) {
            if (size(bucket) != size) {
                return false;
            }

            // Be simple
            int entryOffset = bucket * ENTRY_SIZE;
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
    private static long parseDataPoint(PoorManMap aggrMap, int bucket, MemorySegment data, long offset) {
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
        aggrMap.observe(bucket, value);
        return offset + (decimalSepPos >>> 3) + 3;
    }

    // Tail processing version of the above, do not over-fetch and be simple
    private static long parseDataPointSimple(PoorManMap aggrMap, int bucket, MemorySegment data, long offset) {
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
        aggrMap.observe(bucket, value);
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
            int bucket = aggrMap.indexSimple(data, offset, keySize);
            return parseDataPoint(aggrMap, bucket, data, offset + 1 + keySize);
        }

        // We inline the searching of the value in the hash map
        int hash;
        if (keySize >= Integer.BYTES) {
            hash = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset) +
                    data.get(ValueLayout.JAVA_INT_UNALIGNED, offset + keySize - Integer.BYTES);
        }
        else {
            hash = data.get(ValueLayout.JAVA_BYTE, offset);
        }
        hash = PoorManMap.rehash(hash);
        int bucketMask = aggrMap.capacity - 1;
        int bucket = hash & bucketMask;
        for (;; bucket = (bucket + 1) & bucketMask) {
            if (aggrMap.size(bucket) == 0) {
                aggrMap.insertInto(bucket, data, offset, keySize, hash);
            }

            if (aggrMap.size(bucket) != keySize) {
                continue;
            }

            var nodeKey = ByteVector.fromArray(BYTE_SPECIES, aggrMap.data, aggrMap.keyOffset(bucket));
            long eqMask = line.compare(VectorOperators.EQ, nodeKey).toLong();
            long validMask = -1L >>> -keySize;
            if ((eqMask & validMask) == validMask) {
                break;
            }
        }

        return parseDataPoint(aggrMap, bucket, data, offset + keySize + 1);
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
            for (; data.get(ValueLayout.JAVA_BYTE, offset + keySize) != ';'; keySize++) {
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
