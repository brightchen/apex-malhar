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

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

public class SerdeArrayWithSerializeBuffer<T> implements SerToSerializeBuffer<T[]>
{
  protected Class<T> clazz;
  protected SerializeBuffer buffer;
  protected SerToSerializeBuffer<T> itemSerde;
  
  protected SerdeArrayWithSerializeBuffer()
  {
  }
  
  public SerdeArrayWithSerializeBuffer(Class<T> clazz)
  {
    this.clazz = clazz;
  }

  public SerdeArrayWithSerializeBuffer(Class<T> clazz, LengthValueBuffer buffer)
  {
    this.clazz = clazz;
    this.buffer = buffer;
  }
  
  public SerdeArrayWithSerializeBuffer(SerToSerializeBuffer<T> itemSerde, LengthValueBuffer buffer)
  {
    this.itemSerde = itemSerde;
    this.buffer = buffer;
  }
  
  @Override
  public Slice serialize(T[] objects)
  {
    if (buffer == null) {
      buffer = new LengthValueBuffer();
    }
    serTo(objects, buffer);
    return buffer.toSlice();
  }

  @Override
  public void serTo(T[] objects, SerializeBuffer buffer)
  {
    if (objects.length == 0) {
      return;
    }
    
    //For LengthValueBuffer, need to set the size of the array
    if (buffer instanceof LengthValueBuffer) {
      ((LengthValueBuffer)buffer).setObjectLength(objects.length);
    }
    
    SerToSerializeBuffer<T> serializer = getItemSerToLVBuffer();
    for (T object : objects) {
      serializer.serTo(object, buffer);
    }
  }

  @SuppressWarnings("unchecked")
  protected SerToSerializeBuffer<T> getItemSerToLVBuffer()
  {
    if (itemSerde != null) {
      return itemSerde;
    }
    
    if (String.class.equals(clazz)) {
      itemSerde = (SerToSerializeBuffer<T>)new SerdeStringWithSerializeBuffer();
      return itemSerde;
    }

    throw new UnsupportedOperationException();
  }

  @Override
  public T[] deserialize(Slice slice, MutableInt sliceOffset)
  {
    int numOfElements = GPOUtils.deserializeInt(slice.buffer, sliceOffset);
    if (numOfElements <= 0) {
      throw new IllegalArgumentException(
          "The length of the array is less than or equal to zero. length: " + numOfElements);
    }

    T[] array = createObjectArray(numOfElements);

    for (int index = 0; index < numOfElements; ++index) {
      array[index] = getItemSerToLVBuffer().deserialize(slice, sliceOffset);
    }
    return array;
  }

  @SuppressWarnings("unchecked")
  protected T[] createObjectArray(int length)
  {
    if (String.class == clazz) {
      return (T[])new String[length];
    }

    throw new IllegalArgumentException("Unknow class information: " + clazz);
  }
  
  @Override
  public T[] deserialize(Slice slice)
  {
    return deserialize(slice, new MutableInt(slice.offset));
  }

  @Override
  public void reset()
  {
    if (buffer != null) {
      buffer.reset();
    }
  }

  public void setItemClass(Class<T> clazz)
  {
    this.clazz = clazz;
  }

  @Override
  public void setSerializeBuffer(SerializeBuffer buffer)
  {
    this.buffer = buffer;
  }
  
}
