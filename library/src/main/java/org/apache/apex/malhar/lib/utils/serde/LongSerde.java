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

import java.io.IOException;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.lib.appdata.gpo.GPOUtils;

/**
 * This is an implementation of {@link Serde} which deserializes and serializes integers.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class LongSerde implements Serde<Long>
{
  @Override
  public void serialize(Long value, SerializationBuffer buffer)
  {
    try {
      buffer.writeLong(value);
    } catch (IOException e) {
      throw new RuntimeException("Not suppose to get this exception");
    }
  }

  @Override
  public Long deserialize(byte[] buffer, MutableInt offset, int length)
  {
    long val = GPOUtils.deserializeLong(buffer, offset);
    return val;
  }
}
