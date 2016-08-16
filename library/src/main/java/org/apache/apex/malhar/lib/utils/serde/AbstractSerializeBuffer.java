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

public abstract class AbstractSerializeBuffer implements SerializeBuffer, ResetableWindowListener
{
  protected WindowableByteStream windowableByteStream;
  
  /**
   * write value. it could be part of the value
   * @param value
   */
  @Override
  public void write(byte[] value)
  {
    windowableByteStream.write(value);
  }
  
  /**
   * write value. it could be part of the value
   * 
   * @param value
   * @param offset
   * @param length
   */
  @Override
  public void write(byte[] value, int offset, int length)
  {
    windowableByteStream.write(value, offset, length);
  }


  @Override
  public void setObjectByValue(byte[] value)
  {
    setObjectByValue(value, 0, value.length);
  }

  protected final transient byte[] tmpLengthAsBytes = new byte[4];
  protected final transient MutableInt tmpOffset = new MutableInt(0);
  public void setObjectLength(int length)
  {
    try {
      GPOUtils.serializeInt(length, tmpLengthAsBytes, new MutableInt(0));
      windowableByteStream.write(tmpLengthAsBytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public long size()
  {
    return windowableByteStream.size();
  }
  
  public long capacity()
  {
    return windowableByteStream.capacity();
  }

  /**
   * This method should be called only the whole object has been written
   * @return The slice which represents the object
   */
  public Slice toSlice()
  {
    return windowableByteStream.toSlice();
  }


  /**
   * reset the environment to reuse the resource.
   */
  public void reset()
  {
    windowableByteStream.reset();
  }
  

  @Override
  public void beginWindow(long windowId)
  {
    windowableByteStream.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    windowableByteStream.endWindow();    
  }
  
  /**
   * reset for all windows which window id less or equal input windowId
   * this interface doesn't enforce to call reset window for each windows. Several windows can be reset at the same time.
   * @param windowId
   */
  public void resetUpToWindow(long windowId)
  {
    windowableByteStream.resetUpToWindow(windowId);
  }

  public void release()
  {
    reset();
    windowableByteStream.release();
  }
  
  public WindowableByteStream createWindowableByteStream()
  {
    return new WindowableBlocksStream();
  }

  public WindowableByteStream createWindowableByteStream(int capacity)
  {
    return new WindowableBlocksStream(capacity);
  }
}
