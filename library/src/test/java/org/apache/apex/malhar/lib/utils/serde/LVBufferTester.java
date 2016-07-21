package org.apache.apex.malhar.lib.utils.serde;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

public class LVBufferTester
{
  @Test
  public void testStringArray()
  {
    SerdeArrayWithLVBuffer serde = new SerdeArrayWithLVBuffer(String.class);
    String[] strs = new String[]{"123", "45678", "abcdef"};
    Slice slice = serde.serialize(strs);
    
    SerdeStringSlice serde1 = new SerdeStringSlice();
    Slice[] slices = new Slice[strs.length];
    int index = 0;
    int totalLength = 0;
    for(String str : strs) {
      slices[index] = serde1.serialize(str);
      totalLength += slices[index].length;
      ++index;
    }
    
    byte[] bytes = new byte[totalLength + 4];
    byte[] lengthBytes = GPOUtils.serializeInt(totalLength + 4);
    int offset = 0;
    System.arraycopy(lengthBytes, 0, bytes, 0, 4);
    offset += 4;
    
    for(int i=0; i<slices.length; ++i) {
      System.arraycopy(slices[i].buffer, slices[i].offset, bytes, offset, slices[i].length);
      offset += slices[i].length;
    }
    
    //compare array
    Assert.assertArrayEquals(slice.buffer, bytes);
  }
}
