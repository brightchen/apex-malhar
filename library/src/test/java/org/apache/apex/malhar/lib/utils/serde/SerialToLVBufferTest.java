package org.apache.apex.malhar.lib.utils.serde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.netlet.util.Slice;

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
public class SerialToLVBufferTest
{
  protected final int charNum = 62; 
  protected String[] testData = null;
  protected final Random random = new Random();
  
  @Before
  public void generateTestData()
  {
    int size = random.nextInt(10000) + 1;
    testData = new String[size];
    for (int i = 0; i < size; ++i) {
      char[] chars = new char[random.nextInt(10000) + 1];
      for (int j = 0; j < chars.length; ++j) {
        chars[j] = getRandomChar();
      }

      testData[i] = new String(chars);
    }
  }
  
  protected char getRandomChar()
  {
    int value = random.nextInt(62);
    if (value < 10) {
      return (char)(value + '0');
    } else if (value < 36) {
      return (char)(value + 'A');
    }
    return (char)(value + 'a');
  }
  
  @Test
  public void testSerdeString()
  {
    testSerde(testData, new SerdeStringWithSerializeBuffer(), new StringSerdeVerifier());
  }
  
  @Test
  public void testSerdeArray()
  {
    testSerde(testData, new SerdeArrayWithSerializeBuffer<String>(String.class), new StringArraySerdeVerifier());
  }
  
  
  @Test
  public void testSerdeCollection()
  {
    SerdeCollectionWithSerializeBuffer<String, List<String>> listSerde = new SerdeCollectionWithSerializeBuffer<String, List<String>>(String.class);
    listSerde.setCollectionClass(ArrayList.class);
    testSerde(testData, listSerde, new StringListSerdeVerifier());
  }
  
  
  public <T> void testSerde(String[] strs, SerToSerializeBuffer<T> serde, SerdeVerifier<T> verifier)
  {
    LengthValueBuffer lvBuffer = new LengthValueBuffer();

    for (int i = 0; i < 10; ++i) {
      lvBuffer.beginWindow(i);
      verifier.verifySerde(strs, serde, lvBuffer);
      lvBuffer.endWindow();
      if (i % 3 == 0) {
        lvBuffer.resetUpToWindow(i);
      }
      if (i % 4 == 0) {
        lvBuffer.reset();
      }
    }
    lvBuffer.release();
  }
  
  public static interface SerdeVerifier<T>
  {
    public void verifySerde(String[] datas, SerToSerializeBuffer<T> serde, LengthValueBuffer lvBuffer);
  }
  
  public static class StringSerdeVerifier implements SerdeVerifier<String>
  {
    @Override
    public void verifySerde(String[] datas, SerToSerializeBuffer<String> serde, LengthValueBuffer lvBuffer)
    {
      for (String str : datas) {
        Slice slice = serde.serialize(str);
        Assert.assertTrue("serialize with LVBuffer failed, String: " + str, str.equals(serde.deserialize(slice)));

        serde.serTo(str, lvBuffer);
        Assert.assertTrue("serTo with LVBuffer failed, String: " + str,
            str.equals(serde.deserialize(lvBuffer.toSlice())));
      }
    }
  }
  
  public static class StringArraySerdeVerifier implements SerdeVerifier<String[]>
  {
    @Override
    public void verifySerde(String[] datas, SerToSerializeBuffer<String[]> serde, LengthValueBuffer lvBuffer)
    {
      Slice slice = serde.serialize(datas);
      String[] newStrs = serde.deserialize(slice);
      Assert.assertArrayEquals("serialize array failed.", datas, newStrs);

      serde.serTo(datas, lvBuffer);
      Assert.assertArrayEquals("serTo array failed.", datas, serde.deserialize(lvBuffer.toSlice()));
    }
  }
  
  public static class StringListSerdeVerifier implements SerdeVerifier<List<String>>
  {
    @Override
    public void verifySerde(String[] datas, SerToSerializeBuffer<List<String>> serdeList, LengthValueBuffer lvBuffer)
    {
      List<String> list = Arrays.asList(datas);
      
      Slice slice = serdeList.serialize(list);
      List<String> newStrs = serdeList.deserialize(slice);
      Assert.assertArrayEquals("serialize list failed.", datas, newStrs.toArray(new String[0]));

      serdeList.serTo(list, lvBuffer);
      newStrs = serdeList.deserialize(lvBuffer.toSlice());
      Assert.assertArrayEquals("serTo array failed.", datas, newStrs.toArray(new String[0]));
      lvBuffer.reset();
    }
  }
  
}
