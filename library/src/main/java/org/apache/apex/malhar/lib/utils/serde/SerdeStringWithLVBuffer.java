package org.apache.apex.malhar.lib.utils.serde;

import com.datatorrent.netlet.util.Slice;

public class SerdeStringWithLVBuffer extends SerdeStringSlice implements SerToLVBuffer<String>
{
  //implement with shared buff
  public LVBuffer buffer = new LVBuffer();
  
  @Override
  public Slice serialize(String object)
  {
    serTo(object, buffer);
    return buffer.toSlice();
  }

// implement with tmp buffer  
//  @Override
//  public Slice serialize(String object)
//  {
//    LVBuffer buffer = new LVBuffer();
//    serTo(object, buffer);
//    return buffer.toSlice();
//  }
  
  @Override
  public void serTo(String str, LVBuffer buffer)
  {
    buffer.setObjectWithValue(str.getBytes());
  }



}
