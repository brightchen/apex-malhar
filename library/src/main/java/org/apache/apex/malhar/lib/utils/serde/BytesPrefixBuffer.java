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

/**
 * Unlike LengthValueBuffer which start with the length, 
 * This serialize buffer prefix with a fixed byte array.
 * This SerializeBuffer can be used for key which prefixed by identifier 
 *
 */
public class BytesPrefixBuffer extends AbstractSerializeBuffer
{
  protected byte[] prefix;
  
  /**
   * whether serialize the length of value. It is not necessary when serialize the key.
   * But the previous implementation keep the length. Default is true to compatible with previous implementation.
   */
  protected boolean serializeLength = true;
  
  public BytesPrefixBuffer()
  {
    windowableByteStream = createWindowableByteStream();
  }

  public BytesPrefixBuffer(byte[] prefix)
  {
    setPrefix(prefix);
    setWindowableByteStream(createWindowableByteStream());
  }
      
  public BytesPrefixBuffer(byte[] prefix, int capacity)
  {
    setWindowableByteStream(createWindowableByteStream(capacity));
  }
  
  public BytesPrefixBuffer(byte[] prefix, WindowableByteStream windowableByteStream)
  {
    setPrefix(prefix);
    setWindowableByteStream(windowableByteStream);
  }
  
  /**
   * set value and length. the input value is value only, it doesn't include
   * length information.
   * 
   * @param value
   * @param offset
   * @param length
   */
  @Override
  public void setObjectByValue(byte[] value, int offset, int length)
  {
    if (prefix != null && prefix.length > 0) {
      write(prefix);
    }
    if (serializeLength) {
      setObjectLength(length);
    }
    write(value, offset, length);
  }

  public byte[] getPrefix()
  {
    return prefix;
  }

  public void setPrefix(byte[] prefix)
  {
    this.prefix = prefix;
  }

  public boolean isSerializeLength()
  {
    return serializeLength;
  }

  public void setSerializeLength(boolean serializeLength)
  {
    this.serializeLength = serializeLength;
  }
  
}
