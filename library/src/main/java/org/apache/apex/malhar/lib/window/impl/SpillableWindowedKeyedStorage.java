package org.apache.apex.malhar.lib.window.impl;

import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponentImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by david on 7/15/16.
 */
public class SpillableWindowedKeyedStorage<K, V> implements WindowedStorage.WindowedKeyedStorage<K, V>
{
  @NotNull
  private SpillableStateStore store;
  private SpillableComplexComponentImpl sccImpl;
  private long bucket;
  @NotNull
  private String identifier;
  @NotNull
  private Serde<Window, Slice> windowSerde;
  @NotNull
  private Serde<ImmutablePair<Window, K>, Slice> windowKeyPairSerde;
  @NotNull
  private Serde<K, Slice> keySerde;
  @NotNull
  private Serde<V, Slice> valueSerde;

  protected Spillable.SpillableByteMap<ImmutablePair<Window, K>, V> internValues;
  protected Spillable.SpillableByteArrayListMultimap<Window, K> internKeys;

  public SpillableWindowedKeyedStorage()
  {
  }

  public SpillableWindowedKeyedStorage(long bucket, String identifier,
      Serde<Window, Slice> windowSerde, Serde<ImmutablePair<Window, K>, Slice> windowKeyPairSerde, Serde<K, Slice> keySerde, Serde<V, Slice> valueSerde)
  {
    this.bucket = bucket;
    this.identifier = identifier;
    this.windowSerde = windowSerde;
    this.windowKeyPairSerde = windowKeyPairSerde;
    this.keySerde = keySerde;
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

  public void setIdentifier(String identifier)
  {
    this.identifier = identifier;
  }

  public void setWindowSerde(Serde<Window, Slice> windowSerde)
  {
    this.windowSerde = windowSerde;
  }

  public void setWindowKeyPairSerde(Serde<ImmutablePair<Window, K>, Slice> windowKeyPairSerde)
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
    if (store == null) {
      // provide a default managed state store
      store = new ManagedStateSpillableStateStore();
    }
    sccImpl = new SpillableComplexComponentImpl(store);
    internValues = sccImpl.newSpillableByteMap((identifier + "#values").getBytes(), bucket, windowKeyPairSerde, valueSerde);
    internKeys = sccImpl.newSpillableByteArrayListMultimap((identifier + "#keys").getBytes(), bucket, windowSerde, keySerde);
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
  public void put(Window window, K key, V value)
  {
    if (!internKeys.containsEntry(window, key)) {
      internKeys.put(window, key);
    }
    internValues.put(new ImmutablePair<>(window, key), value);
  }

  @Override
  public Iterable<Map.Entry<K, V>> entrySet(Window window)
  {
    // TBD
    throw new UnsupportedOperationException();
  }

  @Override
  public V get(Window window, K key)
  {
    return internValues.get(new ImmutablePair<>(window, key));
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    store.beforeCheckpoint(windowId);
  }

  @Override
  public void checkpointed(long windowId)
  {
    store.checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
    store.committed(windowId);
  }
}
