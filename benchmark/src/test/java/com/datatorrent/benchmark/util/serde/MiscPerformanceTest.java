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
package com.datatorrent.benchmark.util.serde;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.utils.serde.GenericSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerializationBuffer;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;

public class MiscPerformanceTest
{
  private static final transient Logger logger = LoggerFactory.getLogger(MiscPerformanceTest.class);
  private SerializationBuffer buffer = SerializationBuffer.READ_BUFFER;
  private Random random = new Random();
  private int serdeDataSize = 1000000;


  @Test
  public void testCompareSerdeForString()
  {
    long beginTime = System.currentTimeMillis();
    testSerdeForString(new GenericSerde<String>(String.class));
    long genericSerdeCost = System.currentTimeMillis() - beginTime;
    logger.info("Generic Serde cost for String: {}", genericSerdeCost);

    beginTime = System.currentTimeMillis();
    testSerdeForString(new StringSerde());
    long stringSerdeCost = System.currentTimeMillis() - beginTime;
    logger.info("String Serde cost for String: {}", stringSerdeCost);

    beginTime = System.currentTimeMillis();
    Kryo kryo = new Kryo();
    for(int i=0; i<serdeDataSize; ++i) {
      kryo.writeObject(buffer, ""+random.nextInt(1000));
      buffer.toSlice();
    }
    buffer.release();
    long kryoSerdeCost = System.currentTimeMillis() - beginTime;
    logger.info("Kryo Serde cost for String: {}", kryoSerdeCost);
  }

  protected void testSerdeForString(Serde<String> serde)
  {
    for(int i=0; i<serdeDataSize; ++i) {
      serde.serialize(""+random.nextInt(1000), buffer);
      buffer.toSlice();
    }
    buffer.release();
  }


  @Test
  public void testCompareSerdeForRealCase()
  {
    long beginTime = System.currentTimeMillis();
    GenericSerde<ImmutablePair> serde = new GenericSerde<>();
    for(int i=0; i<serdeDataSize; ++i) {
      serde.serialize(generatePair(beginTime), buffer);
      buffer.toSlice();
    }
    buffer.release();
    long genericSerdeCost = System.currentTimeMillis() - beginTime;
    logger.info("Generic Serde cost for ImmutablePair: {}", genericSerdeCost);

    beginTime = System.currentTimeMillis();
    Kryo kryo = new Kryo();
    for(int i=0; i<serdeDataSize; ++i) {
      kryo.writeObject(buffer, generatePair(beginTime));
      buffer.toSlice();
    }
    buffer.release();
    long kryoSerdeCost = System.currentTimeMillis() - beginTime;
    logger.info("Kryo Serde cost for ImmutablePair: {}", kryoSerdeCost);
  }

  @Test
  public void testMap()
  {

    long now = System.currentTimeMillis();

    {
      Map<ImmutablePair<Window, String>, Long> windowKeyToValueMap = Maps.newHashMap();
      windowKeyToValueMap.put(
          new ImmutablePair(new Window.TimeWindow<>(1, 10), "100"), 10L);
      Assert.assertTrue(windowKeyToValueMap.get(new ImmutablePair(new Window.TimeWindow<>(1, 10), "100")) == 10L);
    }

    final int size = 1000000;
    {
      Map<ImmutablePair<Window, String>, Long> windowKeyToValueMap = Maps.newHashMap();

      long beginTime = System.currentTimeMillis();
      for(int i=0; i<size; ++i) {
        windowKeyToValueMap.put(generatePair(now), (long)random.nextInt(10));
      }
      logger.info("insert ImmutablePair cost: {}", System.currentTimeMillis() - beginTime);
    }

    {
      Map<Pair<Window, String>, Long> windowKeyToValueMap = Maps.newHashMap();

      long beginTime = System.currentTimeMillis();
      for (int i = 0; i < size; ++i) {
        windowKeyToValueMap.put(new Pair(new Window.TimeWindow<>(now + random.nextInt(100), random.nextInt(100)),
                "" + random.nextInt(1000)), (long)random.nextInt(10));
      }
      logger.info("insert pair cost: {}", System.currentTimeMillis() - beginTime);
    }
  }

  protected ImmutablePair generatePair(long now)
  {
    return new ImmutablePair(new Window.TimeWindow<>(now + random.nextInt(100), random.nextInt(100)), "" + random.nextInt(1000));
  }

  protected static class Pair<F, S> implements Serializable
  {
    private static final long serialVersionUID = 731157267102567944L;
    public final F first;
    public final S second;

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 41 * hash + (this.first != null ? this.first.hashCode() : 0);
      hash = 41 * hash + (this.second != null ? this.second.hashCode() : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      @SuppressWarnings("unchecked")
      final Pair<F, S> other = (Pair<F, S>)obj;
      if (this.first != other.first && (this.first == null || !this.first.equals(other.first))) {
        return false;
      }
      if (this.second != other.second && (this.second == null || !this.second.equals(other.second))) {
        return false;
      }
      return true;
    }

    public Pair(F first, S second)
    {
      this.first = first;
      this.second = second;
    }

    public F getFirst()
    {
      return first;
    }

    public S getSecond()
    {
      return second;
    }

    @Override
    public String toString()
    {
      return "[" + first + "," + second + "]";
    }
  }
}
