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
  public void testBlockStream()
  {
    BlockStream bs = new BlockStream();
    for (int tryTime = 0; tryTime < 10; ++tryTime) {
      bs.reset();
      List<Slice> slices = Lists.newArrayList();
      List<byte[]> list = generateList();
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
  }
  
  protected List<byte[]> generateList()
  {
    List<byte[]> list = Lists.newArrayList();
    int size = random.nextInt(10000) + 1;
    for(int i=0; i<size; i++) {
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
