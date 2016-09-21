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

import java.util.ArrayList;
import java.util.List;

/**
 * The serialize was implemented by this class, the deserialize was inherited from super class
 *
 * @param <T> The type of serializer for item
 */
public class SerdeListSliceWithSerializeBuffer<T> extends SerdeCollectionWithSerializeBuffer<T, List<T>>
{
  protected SerToSerializeBuffer<T> itemSerTo;
  protected SerializeBuffer buffer;
  
  //for kyro
  protected SerdeListSliceWithSerializeBuffer()
  {
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public SerdeListSliceWithSerializeBuffer(SerToSerializeBuffer<T> itemSerde)
  {
    super(itemSerde);
    setCollectionClass((Class)ArrayList.class);
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public SerdeListSliceWithSerializeBuffer(SerToSerializeBuffer<T> itemSerde, SerializeBuffer buffer)
  {
    super(itemSerde, buffer);
    setCollectionClass((Class)ArrayList.class);
  }
}
