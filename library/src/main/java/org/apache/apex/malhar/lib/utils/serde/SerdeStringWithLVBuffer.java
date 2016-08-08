package org.apache.apex.malhar.lib.utils.serde;

import com.google.common.base.Preconditions;

import com.datatorrent.netlet.util.Slice;

public class SerdeStringWithLVBuffer extends SerdeStringSlice implements SerToLVBuffer<String>
{
  //implement with shared buff
  protected LVBuffer buffer;
  
  /**
   * if don't use SerdeStringWithLVBuffer.serialize(String), can ignore LVBuffer
   */
  public SerdeStringWithLVBuffer()
  {
  }
  
  public SerdeStringWithLVBuffer(LVBuffer buffer)
  {
    this.buffer = Preconditions.checkNotNull(buffer);
  }
  
  @Override
  public Slice serialize(String object)
  {
    if(buffer == null) {
      buffer = new LVBuffer();
    }
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

  @Override
  public void reset()
  {
    if(buffer != null) {
      buffer.reset();
    }
  }
}
