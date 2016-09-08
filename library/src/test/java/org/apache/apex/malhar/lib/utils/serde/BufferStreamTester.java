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
package org.apache.apex.malhar.lib.utils.serde;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.netlet.util.Slice;

public class BufferStreamTester
{
  protected Random random = new Random();
  
  @Test
  public void testWindowableBlockStream()
  {
    WindowableBlocksStream bs = new WindowableBlocksStream();
    List<byte[]> totalList = Lists.newArrayList();
    List<Slice> slices = Lists.newArrayList();

    for (int windowId = 0; windowId < 10; ++windowId) {
      List<byte[]> list = generateList();
      totalList.addAll(list);
      
      bs.beginWindow(windowId);
      writeToByteStream(bs, list, slices);
      bs.endWindow();
      
      if (windowId % 2 != 0) {
        verify(totalList, slices);

        bs.resetUpToWindow(windowId);
        totalList.clear();
        slices.clear();
      }
    }
  }
  
  @Test
  public void testBlockStream()
  {
    BlocksStream bs = new BlocksStream();
    List<byte[]> totalList = Lists.newArrayList();
    List<Slice> slices = Lists.newArrayList();

    for (int tryTime = 0; tryTime < 10; ++tryTime) {
      List<byte[]> list = generateList();
      totalList.addAll(list);
      
      writeToByteStream(bs, list, slices);
      
      if (tryTime % 2 != 0) {
        verify(totalList, slices);
        
        bs.reset();
        totalList.clear();
        slices.clear();
      }
      
    }
  }
  
  protected void writeToByteStream(ByteStream bs, List<byte[]> list, List<Slice> slices)
  {
    for (byte[] bytes : list) {
      int times = random.nextInt(100) + 1;
      int remainLen = bytes.length;
      int offset = 0;
      while (times > 0 && remainLen > 0) {
        int avgSubLen = remainLen / times;
        times--;
        if (avgSubLen == 0) {
          bs.write(bytes, offset, remainLen);
          break;
        }

        int writeLen = remainLen;
        if (times != 0) {
          int subLen = random.nextInt(avgSubLen * 2);
          writeLen = Math.min(subLen, remainLen);
        }
        bs.write(bytes, offset, writeLen);

        offset += writeLen;
        remainLen -= writeLen;
      }
      slices.add(bs.toSlice());
    }
  }

  protected void verify(List<byte[]> list, List<Slice> slices)
  {
    //verify
    Assert.assertTrue("size not equal.", list.size() == slices.size());

    for (int i = 0; i < list.size(); ++i) {
      byte[] bytes = list.get(i);
      byte[] newBytes = slices.get(i).toByteArray();
      if (!Arrays.equals(bytes, newBytes)) {
        Assert.assertArrayEquals(bytes, newBytes);
      }
    }
  }
  
  protected List<byte[]> generateList()
  {
    List<byte[]> list = Lists.newArrayList();
    int size = random.nextInt(10000) + 1;
    for (int i = 0; i < size; i++) {
      list.add(generateByteArray());
    }
    return list;
  }

  protected byte[] generateByteArray()
  {
    int len = random.nextInt(10000) + 1;
    byte[] bytes = new byte[len];
    random.nextBytes(bytes);
    return bytes;
  }
}
