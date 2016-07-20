package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.netlet.util.Slice;

public class SerdeArrayWithLVBuffer<T> implements Serde<T[], Slice>, SerToLVBuffer<T[]>
{
  protected Class<T> clazz;
  public SerdeArrayWithLVBuffer(Class<T> clazz)
  {
    this.clazz = clazz;
  }
  
  @Override
  public Slice serialize(T[] objects)
  {
    LVBuffer buffer = new LVBuffer();
    serTo(objects, buffer);
    return buffer.toSlice();
  }

  @Override
  public void serTo(T[] objects, LVBuffer buffer)
  {
    buffer.setObjectLength(objects.length);
    SerToLVBuffer<T> serializer = getSerToLVBuffer(clazz);
    for(T object : objects) {
      serializer.serTo(object, buffer);
    }
  }
  
  protected SerToLVBuffer<T> getSerToLVBuffer(Class<T> clazz)
  {
    if(String.class.equals(clazz))
      return (SerToLVBuffer)new SerdeStringWithLVBuffer();
    
    throw new UnsupportedOperationException();
  }

  @Override
  public T[] deserialize(Slice object, MutableInt offset)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public T[] deserialize(Slice object)
  {
    // TODO Auto-generated method stub
    return null;
  }
  
}
