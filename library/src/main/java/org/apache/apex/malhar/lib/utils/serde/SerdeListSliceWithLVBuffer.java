package org.apache.apex.malhar.lib.utils.serde;

import java.util.List;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

import com.datatorrent.netlet.util.Slice;

public class SerdeListSliceWithLVBuffer<T> extends SerdeListSlice<T> implements SerToLVBuffer<List<T>>
{
  protected SerToLVBuffer<T> itemSerTo;
  
  protected SerdeListSliceWithLVBuffer()
  {
    // for Kryo
  }

  public SerdeListSliceWithLVBuffer(@NotNull SerToLVBuffer<T> serde)
  {
    this.itemSerTo = Preconditions.checkNotNull(itemSerTo);
  }

  @Override
  public Slice serialize(List<T> objects)
  {
    LVBuffer buffer = new LVBuffer();
    serTo(objects, buffer);
    return buffer.toSlice();
  }
  
  @Override
  public void serTo(List<T> objects, LVBuffer buffer)
  {
    buffer.setObjectLength(objects.size());
    for (T object : objects) {
      itemSerTo.serTo(object, buffer);;
    }
  }

}
