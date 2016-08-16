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

import com.datatorrent.netlet.util.Slice;

public interface SerializeBuffer
{
  /**
   * write value. it could be part of the value
   * @param value
   */
  public void write(byte[] value);

  
  /**
   * write value. it could be part of the value
   * 
   * @param value
   * @param offset
   * @param length
   */
  public void write(byte[] value, int offset, int length);


  /**
   * set value and length. the input value is value only, it doesn't include
   * length information.
   * 
   * @param value
   * @param offset
   * @param length
   */
  public void setObjectByValue(byte[] value, int offset, int length);


  public void setObjectByValue(byte[] value);
  
  /**
   * reset the environment to reuse the resource.
   */
  public void reset();
  
  public void release();
  
  /**
   * This method should be called only the whole object has been written
   * @return The slice which represents the object
   */
  public Slice toSlice();
  
  
}
