package org.apache.apex.malhar.lib.utils.serde;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

public class LVBufferTester
{
  public static transient final Logger logger = LoggerFactory.getLogger(LVBufferTester.class);
      
  @Test
  public void testStringSerialize()
  {
    String[] strs = new String[]{"123", "45678", "abcdef"};
    SerdeStringSlice serde = new SerdeStringSlice();
    for(int i=0; i<3; ++i) {
      for(String str : strs) {
        Slice slice = serde.serialize(str);
        String str1 = serde.deserialize(slice);
        Assert.assertTrue(str.equals(str1));
      }
    }
  }
  
  //around 7695/mill-second when reset for each loop
  @Test
  public void testStringSerializeBenchmark()
  {
    int loopCount = 10000000;
    String[] strs = new String[]{"123", "45678", "abcdef", "dfaqecdgr"};
    SerdeStringWithLVBuffer serde = new SerdeStringWithLVBuffer();
    
    long startTime = System.currentTimeMillis();
    for(int i=0; i<loopCount; ++i) {
      for(String str : strs) {
        serde.serialize(str);
      }
      if(loopCount % 1000000 == 0)
        serde.reset();
    }
    long spentTime = System.currentTimeMillis() - startTime;
    
    logger.info("Spent {} mills for {} operation. average: {}", spentTime, strs.length*loopCount, strs.length*loopCount/spentTime);
  }

  // around 10111/mill-second
//  @Test
//  public void testStringSerializeOldBenchmark()
//  {
//    int loopCount = 10000000;
//    String[] strs = new String[]{"123", "45678", "abcdef", "dfaqecdgr"};
//    SerdeStringSlice serde1 = new SerdeStringSlice();
//    
//    long startTime = System.currentTimeMillis();
//    for(int i=0; i<loopCount; ++i) {
//      for(String str : strs) {
//        serde1.serializeOld(str);
//      }
//    }
//    long spentTime = System.currentTimeMillis() - startTime;
//    
//    logger.info("Spent {} mills for {} operation. average: {}", spentTime, strs.length*loopCount, strs.length*loopCount/spentTime);
//  }

  
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
