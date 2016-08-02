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

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponent;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerdeKryoSlice;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * Implementation of WindowedKeyedStorage using {@link Spillable} data structures
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class SpillableWindowedKeyedStorage<K, V> implements WindowedStorage.WindowedKeyedStorage<K, V>
{
  @NotNull
  private SpillableComplexComponent scc;
  private long bucket;
  private Serde<Window, Slice> windowSerde;
  private Serde<Pair<Window, K>, Slice> windowKeyPairSerde;
  private Serde<K, Slice> keySerde;
  private Serde<V, Slice> valueSerde;

  protected Spillable.SpillableByteMap<Pair<Window, K>, V> internValues;
  protected Spillable.SpillableByteArrayListMultimap<Window, K> internKeys;

  private class KVIterator implements Iterator<Map.Entry<K, V>>
  {
    final Window window;
    final List<K> keys;
    Iterator<K> iterator;

    KVIterator(Window window)
    {
      this.window = window;
      this.keys = internKeys.get(window);
      this.iterator = this.keys.iterator();
    }

    @Override
    public boolean hasNext()
    {
      return iterator.hasNext();
    }

    @Override
    public Map.Entry<K, V> next()
    {
      K key = iterator.next();
      return new AbstractMap.SimpleEntry<>(key, internValues.get(new ImmutablePair<>(window, key)));
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  public SpillableWindowedKeyedStorage()
  {
  }

  public SpillableWindowedKeyedStorage(long bucket,
      Serde<Window, Slice> windowSerde, Serde<Pair<Window, K>, Slice> windowKeyPairSerde, Serde<K, Slice> keySerde, Serde<V, Slice> valueSerde)
  {
    this.bucket = bucket;
    this.windowSerde = windowSerde;
    this.windowKeyPairSerde = windowKeyPairSerde;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  public void setSpillableComplexComponent(SpillableComplexComponent scc)
  {
    this.scc = scc;
  }

  public void setBucket(long bucket)
  {
    this.bucket = bucket;
  }

  public void setWindowSerde(Serde<Window, Slice> windowSerde)
  {
    this.windowSerde = windowSerde;
  }

  public void setWindowKeyPairSerde(Serde<Pair<Window, K>, Slice> windowKeyPairSerde)
  {
    this.windowKeyPairSerde = windowKeyPairSerde;
  }

  public void setValueSerde(Serde<V, Slice> valueSerde)
  {
    this.valueSerde = valueSerde;
  }

  @Override
  public boolean containsWindow(Window window)
  {
    return internKeys.containsKey(window);
  }

  @Override
  public long size()
  {
    return internKeys.size();
  }

  @Override
  public void remove(Window window)
  {
    List<K> keys = internKeys.get(window);
    for (K key : keys) {
      internValues.remove(new ImmutablePair<>(window, key));
    }
    internKeys.removeAll(window);
  }

  @Override
  public void migrateWindow(Window fromWindow, Window toWindow)
  {
    List<K> keys = internKeys.get(fromWindow);
    internValues.remove(toWindow);
    for (K key : keys) {
      internKeys.put(toWindow, key);
      ImmutablePair<Window, K> oldKey = new ImmutablePair<>(fromWindow, key);
      ImmutablePair<Window, K> newKey = new ImmutablePair<>(toWindow, key);

      V value = internValues.get(oldKey);
      internValues.remove(oldKey);
      internValues.put(newKey, value);
    }
    internKeys.removeAll(fromWindow);
  }


  @Override
  public void setup(Context.OperatorContext context)
  {
    if (bucket == 0) {
      // choose a bucket that is guaranteed to be unique in Apex
      bucket = (context.getValue(Context.DAGContext.APPLICATION_NAME) + "#" + context.getId()).hashCode();
    }
    // set default serdes
    if (windowSerde == null) {
      windowSerde = new SerdeKryoSlice<>();
    }
    if (windowKeyPairSerde == null) {
      windowKeyPairSerde = new SerdeKryoSlice<>();
    }
    if (keySerde == null) {
      keySerde = new SerdeKryoSlice<>();
    }
    if (valueSerde == null) {
      valueSerde = new SerdeKryoSlice<>();
    }

    if (internValues == null) {
      internValues = scc.newSpillableByteMap(bucket, windowKeyPairSerde, valueSerde);
    }
    if (internKeys == null) {
      internKeys = scc.newSpillableByteArrayListMultimap(bucket, windowSerde, keySerde);
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void put(Window window, K key, V value)
  {
    if (!internKeys.containsEntry(window, key)) {
      internKeys.put(window, key);
    }
    internValues.put(new ImmutablePair<>(window, key), value);
  }

  @Override
  public Iterable<Map.Entry<K, V>> entrySet(final Window window)
  {
    return new Iterable<Map.Entry<K, V>>()
    {
      @Override
      public Iterator<Map.Entry<K, V>> iterator()
      {
        return new KVIterator(window);
      }
    };
  }

  @Override
  public V get(Window window, K key)
  {
    return internValues.get(new ImmutablePair<>(window, key));
  }
}
