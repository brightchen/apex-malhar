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
package org.apache.apex.malhar.lib.window.impl;

import java.util.Iterator;
import java.util.Map;

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponentImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerdeKryoSlice;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * This is an implementation of WindowedPlainStorage that makes use of {@link Spillable} data structures
 *
 * @param <T> Type of the value per window
 */
public class SpillableWindowedPlainStorage<T> implements WindowedStorage.WindowedPlainStorage<T>
{
  private SpillableStateStore store;
  private SpillableComplexComponentImpl sccImpl;
  private long bucket;
  private Serde<Window, Slice> windowSerde;
  private Serde<T, Slice> valueSerde;

  protected Spillable.SpillableByteMap<Window, T> internMap;

  public SpillableWindowedPlainStorage()
  {
  }

  public SpillableWindowedPlainStorage(long bucket, Serde<Window, Slice> windowSerde, Serde<T, Slice> valueSerde)
  {
    this.bucket = bucket;
    this.windowSerde = windowSerde;
    this.valueSerde = valueSerde;
  }

  public void setStore(SpillableStateStore store)
  {
    this.store = store;
  }

  public void setBucket(long bucket)
  {
    this.bucket = bucket;
  }

  public void setWindowSerde(Serde<Window, Slice> windowSerde)
  {
    this.windowSerde = windowSerde;
  }

  public void setValueSerde(Serde<T, Slice> valueSerde)
  {
    this.valueSerde = valueSerde;
  }

  @Override
  public void put(Window window, T value)
  {
    internMap.put(window, value);
  }

  @Override
  public T get(Window window)
  {
    return internMap.get(window);
  }

  @Override
  public Iterable<Map.Entry<Window, T>> entrySet()
  {
    return internMap.entrySet();
  }

  @Override
  public Iterator<Map.Entry<Window, T>> iterator()
  {
    return internMap.entrySet().iterator();
  }

  @Override
  public boolean containsWindow(Window window)
  {
    return internMap.containsKey(window);
  }

  @Override
  public long size()
  {
    return internMap.size();
  }

  @Override
  public void remove(Window window)
  {
    internMap.remove(window);
  }

  @Override
  public void migrateWindow(Window fromWindow, Window toWindow)
  {
    internMap.put(toWindow, internMap.remove(fromWindow));
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (store == null) {
      // provide a default store
      store = new ManagedStateSpillableStateStore();
    }
    if (bucket == 0) {
      // choose a bucket that is almost guaranteed to be unique
      bucket = (context.getValue(Context.DAGContext.APPLICATION_NAME) + "#" + context.getId()).hashCode();
    }
    // set default serdes
    if (windowSerde == null) {
      windowSerde = new SerdeKryoSlice<>();
    }
    if (valueSerde == null) {
      valueSerde = new SerdeKryoSlice<>();
    }
    if (sccImpl == null) {
      sccImpl = new SpillableComplexComponentImpl(store);
      internMap = sccImpl.newSpillableByteMap(bucket, windowSerde, valueSerde);
    }
    sccImpl.setup(context);
  }

  @Override
  public void teardown()
  {
    sccImpl.teardown();
  }

  @Override
  public void beginApexWindow(long windowId)
  {
    sccImpl.beginWindow(windowId);
  }

  @Override
  public void endApexWindow()
  {
    sccImpl.endWindow();
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    sccImpl.beforeCheckpoint(windowId);
  }

  @Override
  public void checkpointed(long windowId)
  {
    sccImpl.checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
    sccImpl.committed(windowId);
  }
}
