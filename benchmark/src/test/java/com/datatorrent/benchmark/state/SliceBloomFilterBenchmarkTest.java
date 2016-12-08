/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.benchmark.state;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import org.apache.apex.malhar.lib.state.managed.SliceBloomFilter;
import org.apache.apex.malhar.lib.utils.serde.AffixSerde;
import org.apache.apex.malhar.lib.utils.serde.GenericSerde;
import org.apache.apex.malhar.lib.utils.serde.SerializationBuffer;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import com.datatorrent.netlet.util.Slice;

public class SliceBloomFilterBenchmarkTest
{
  private static int bloomFilterDefaultBitSize = 1000000;

  private int bloomFilterBitSize = bloomFilterDefaultBitSize;
  protected transient SerializationBuffer keyBufferForRead = SerializationBuffer.READ_BUFFER;
  private Random random = new Random();
  private int tupleSize = 10000000;
  private int valueRange = 100000;
  @Test
  public void testSliceBloomFilter()
  {
    SliceBloomFilter bloomFilter = new SliceBloomFilter(bloomFilterBitSize, 0.99);
    GenericSerde<String> serde = new GenericSerde<String>();
    AffixSerde<String> metaKeySerde = new AffixSerde<String>(null, serde, new byte[] { (byte)1, (byte)2 });

    long beginTime = System.currentTimeMillis();
    int putCount = 0;
    for (int i = 0; i < tupleSize; ++i) {
      metaKeySerde.serialize("" + (i%valueRange), keyBufferForRead);
      Slice slice = keyBufferForRead.toSlice();
//      Slice slice = new Slice(ByteBuffer.allocate(4).putInt(i%valueRange).array());
      if (!bloomFilter.mightContain(slice)) {
        ++putCount;
        bloomFilter.put(slice);
      }
      if (i % 1000000 == 0) {
        keyBufferForRead.reset();;
        keyBufferForRead.getWindowedBlockStream().releaseAllFreeMemory();
        System.out.println("stream capacity: " + keyBufferForRead.getWindowedBlockStream().capacity());
        System.out.println("BitSet size: " + bloomFilter.getBitSet().size());
      }
    }
    System.out.println("SliceBloomFilter cost time: " + (System.currentTimeMillis() - beginTime) + "; putCount: " + putCount);
  }

  @Test
  public void testHadoopBloomFilter()
  {
    BloomFilter bloomFilter = new BloomFilter(bloomFilterBitSize, 2, Hash.MURMUR_HASH);
    GenericSerde<String> serde = new GenericSerde<String>();
    AffixSerde<String> metaKeySerde = new AffixSerde<String>(null, serde, new byte[] { (byte)1, (byte)2 });

    long beginTime = System.currentTimeMillis();
    int putCount = 0;
    for (int i = 0; i < tupleSize; ++i) {
//      metaKeySerde.serialize("" + (i%100), keyBufferForRead);
//      Slice slice = keyBufferForRead.toSlice();
      Slice slice = new Slice(ByteBuffer.allocate(4).putInt(i%valueRange).array());
      if (!bloomFilter.membershipTest(new Key(slice.toByteArray()))) {
        ++putCount;
        bloomFilter.add((new Key(slice.toByteArray())));
      }
      keyBufferForRead.clear();
    }
    System.out.println("Hadoop BloomFilter cost time: " + (System.currentTimeMillis() - beginTime) + "; putCount: " + putCount);
  }
}
