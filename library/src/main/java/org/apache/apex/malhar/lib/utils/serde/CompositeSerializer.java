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

import javax.validation.constraints.NotNull;

import com.datatorrent.netlet.util.Slice;

public class CompositeSerializer<T1, T2>
{
  protected SerToSerializeBuffer<T1> serializer1;
  protected SerToSerializeBuffer<T2> serializer2;
  
  @NotNull
  protected LengthValueBuffer buffer;
  
  //for Kyro
  protected CompositeSerializer()
  {
  }

  public CompositeSerializer(SerToSerializeBuffer<T1> serializer1, SerToSerializeBuffer<T2> serializer2, @NotNull LengthValueBuffer buffer)
  {
    this.serializer1 = serializer1;
    this.serializer2 = serializer2;
    this.buffer = buffer;
  }
  
  public Slice serialize(T1 value1, T2 value2)
  {
    serializeFirst(value1);
    serializeSecond(value2);
    return buffer.toSlice();
  }
  
  protected void serializeFirst(T1 value1)
  {
    serializer1.serTo(value1, buffer);
  }
  
  protected void serializeSecond(T2 value2)
  {
    serializer2.serTo(value2, buffer);
  }
}
