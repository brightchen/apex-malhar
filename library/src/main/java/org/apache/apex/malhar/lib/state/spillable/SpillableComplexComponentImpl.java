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
package org.apache.apex.malhar.lib.state.spillable;

import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.utils.serde.Serde;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 6/12/16.
 */
public class SpillableComplexComponentImpl implements SpillableComplexComponent
{
  private List<SpillableComponent> componentList = Lists.newArrayList();

  private boolean isRunning = false;
  private boolean isInWindow = false;

  @NotNull
  private SpillableStateStore store;

  @NotNull
  private SpillableIdentifierGenerator identifierGenerator;

  public SpillableComplexComponentImpl(SpillableStateStore store)
  {
    this(store, new SequentialSpillableIdentifierGenerator());
  }

  public SpillableComplexComponentImpl(SpillableStateStore store, SpillableIdentifierGenerator identifierGenerator)
  {
    this.store = Preconditions.checkNotNull(store);
    this.identifierGenerator = Preconditions.checkNotNull(identifierGenerator);
  }

  public <T> SpillableArrayList<T> newSpillableArrayList(long bucket, Serde<T, Slice> serde)
  {
    SpillableArrayListImpl<T> list = new SpillableArrayListImpl<T>(bucket, identifierGenerator.next(), store, serde);
    componentList.add(list);
    return list;
  }

  public <T> SpillableArrayList<T> newSpillableArrayList(byte[] identifier, long bucket, Serde<T, Slice> serde)
  {
    identifierGenerator.register(identifier);
    SpillableArrayListImpl<T> list = new SpillableArrayListImpl<T>(bucket, identifier, store, serde);
    componentList.add(list);
    return list;
  }

  public <K, V> SpillableByteMap<K, V> newSpillableByteMap(long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    SpillableByteMapImpl<K, V> map = new SpillableByteMapImpl<K, V>(store, identifierGenerator.next(),
        bucket, serdeKey, serdeValue);
    componentList.add(map);
    return map;
  }

  public <K, V> SpillableByteMap<K, V> newSpillableByteMap(byte[] identifier, long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    identifierGenerator.register(identifier);
    SpillableByteMapImpl<K, V> map = new SpillableByteMapImpl<K, V>(store, identifier, bucket, serdeKey, serdeValue);
    componentList.add(map);
    return map;
  }

  public <K, V> SpillableByteArrayListMultimap<K, V> newSpillableByteArrayListMultimap(long bucket, Serde<K,
      Slice> serdeKey, Serde<V, Slice> serdeValue)
  {
    SpillableByteArrayListMultimapImpl<K, V> map = new SpillableByteArrayListMultimapImpl<K, V>(store,
        identifierGenerator.next(), bucket, serdeKey, serdeValue);
    componentList.add(map);
    return map;
  }

  public <K, V> SpillableByteArrayListMultimap<K, V> newSpillableByteArrayListMultimap(byte[] identifier, long bucket,
      Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    identifierGenerator.register(identifier);
    SpillableByteArrayListMultimapImpl<K, V> map = new SpillableByteArrayListMultimapImpl<K, V>(store,
        identifier, bucket, serdeKey, serdeValue);
    componentList.add(map);
    return map;
  }

  public <T> SpillableByteMultiset<T> newSpillableByteMultiset(long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  public <T> SpillableByteMultiset<T> newSpillableByteMultiset(byte[] identifier, long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  public <T> SpillableQueue<T> newSpillableQueue(long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  public <T> SpillableQueue<T> newSpillableQueue(byte[] identifier, long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    store.setup(context);
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.setup(context);
    }
    isRunning = true;
  }

  @Override
  public void beginWindow(long windowId)
  {
    store.beginWindow(windowId);
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.beginWindow(windowId);
    }
    isInWindow = true;
  }

  @Override
  public void endWindow()
  {
    isInWindow = false;
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.endWindow();
    }
    store.endWindow();
  }

  @Override
  public void teardown()
  {
    isRunning = false;
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.teardown();
    }
    store.teardown();
  }

  @Override
  public void beforeCheckpoint(long l)
  {
    store.beforeCheckpoint(l);
  }

  @Override
  public void checkpointed(long l)
  {
    store.checkpointed(l);
  }

  @Override
  public void committed(long l)
  {
    store.committed(l);
  }

  @Override
  public boolean isRunning()
  {
    return isRunning;
  }

  @Override
  public boolean isInWindow()
  {
    return isInWindow;
  }
}
