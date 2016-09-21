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

import java.util.Collection;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

public class SerdeCollectionWithSerializeBuffer<T, C extends Collection<T>> implements SerToSerializeBuffer<C>
{
  protected SerializeBuffer buffer;
  protected SerToSerializeBuffer<T> itemSerde;
  
  //at least one of collectionClass and collection should be set.
  //collection have higher priority than collectionClass
  protected Class<? extends C> collectionClass;
  protected C collection;
  
  //for kyro
  protected SerdeCollectionWithSerializeBuffer()
  {
  }

  public SerdeCollectionWithSerializeBuffer(SerToSerializeBuffer<T> itemSerde)
  {
    this(itemSerde, new LengthValueBuffer());
  }
  
  public SerdeCollectionWithSerializeBuffer(SerToSerializeBuffer<T> itemSerde, SerializeBuffer buffer)
  {
    this.itemSerde = itemSerde;
    this.buffer = buffer;
  }
  
  @Override
  public Slice serialize(C objects)
  {
    if (buffer == null) {
      buffer = new LengthValueBuffer();
    }
    serTo(objects, buffer);
    return buffer.toSlice();
  }

  @Override
  public void serTo(C objects, SerializeBuffer buffer)
  {
    if (objects.size() == 0) {
      return;
    }
    
    //For LengthValueBuffer, need to set the size
    if (buffer instanceof LengthValueBuffer) {
      ((LengthValueBuffer)buffer).setObjectLength(objects.size());
    }

    SerToSerializeBuffer<T> serializer = getItemSerToLVBuffer();
    for (T object : objects) {
      serializer.serTo(object, buffer);
    }
  }

  @SuppressWarnings("unchecked")
  protected SerToSerializeBuffer<T> getItemSerToLVBuffer()
  {
    return itemSerde;
  }

  @Override
  public C deserialize(Slice slice, MutableInt sliceOffset)
  {
    int numOfElements = GPOUtils.deserializeInt(slice.buffer, sliceOffset);
    if (numOfElements <= 0) {
      throw new IllegalArgumentException(
          "The length of the array is less than or equal to zero. length: " + numOfElements);
    }

    C collection = createObjectCollection(numOfElements);

    for (int index = 0; index < numOfElements; ++index) {
      collection.add(getItemSerToLVBuffer().deserialize(slice, sliceOffset));
    }
    return collection;
  }

  protected C createObjectCollection(int length)
  {
    if (collection != null) {
      return collection;
    }
    
    if (collectionClass == null) {
      throw new IllegalArgumentException("Both collection and collectionClass.");
    }

    try {
      return collectionClass.newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException("Can't instancial collection class: " + collectionClass.getName());
    }
  }
  
  @Override
  public C deserialize(Slice slice)
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

  public Class<? extends C> getCollectionClass()
  {
    return collectionClass;
  }

  /**
   * The class of the output collection. It should be C or it's sub-class.
   * Due to the type erasure of runtime, the caller probably can't get the class with type info at runtime
   * So, do runtime check instead of compile time check
   * 
   * @param collectionClass
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void  setCollectionClass(Class<? extends C> collectionClass)
  {
    if (collectionClass.isInterface()) {
      throw new IllegalArgumentException("collectionClass should be a class instead of interface.");
    }

    this.collectionClass = (Class)collectionClass;
  }

  
  public C getCollection()
  {
    return collection;
  }

  public void setCollection(C collection)
  {
    this.collection = collection;
  }

  @Override
  public void setSerializeBuffer(SerializeBuffer buffer)
  {
    this.buffer = buffer;
  }
}
