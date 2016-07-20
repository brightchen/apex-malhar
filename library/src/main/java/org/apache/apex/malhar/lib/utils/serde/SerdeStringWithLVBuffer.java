package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.netlet.util.Slice;

public class SerdeStringWithLVBuffer implements Serde<String, Slice>, SerToLVBuffer<String>
{
  @Override
  public Slice serialize(String object)
  {
    LVBuffer buffer = new LVBuffer();
    serTo(object, buffer);
    return buffer.toSlice();
  }
  
  @Override
  public void serTo(String str, LVBuffer buffer)
  {
    buffer.setObjectWithValue(str.getBytes());
  }
  
  @Override
  public String deserialize(Slice object, MutableInt offset)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String deserialize(Slice object)
  {
    // TODO Auto-generated method stub
    return null;
  }



}
