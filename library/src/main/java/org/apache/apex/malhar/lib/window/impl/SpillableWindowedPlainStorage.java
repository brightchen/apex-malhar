package org.apache.apex.malhar.lib.window.impl;

import java.util.Iterator;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponentImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * This is an implementation of WindowedPlainStorage that makes use of Spillable data structures
 *
 * @param <T> Type of the value per window
 */
public class SpillableWindowedPlainStorage<T> implements WindowedStorage.WindowedPlainStorage<T>
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
  private Serde<T, Slice> valueSerde;

  protected Spillable.SpillableByteMap<Window, T> internMap;

  public SpillableWindowedPlainStorage()
  {
  }

  public SpillableWindowedPlainStorage(long bucket, String identifier, Serde<Window, Slice> windowSerde, Serde<T, Slice> valueSerde)
  {
    this.bucket = bucket;
    this.identifier = identifier;
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

  public void setIdentifier(String identifier)
  {
    this.identifier = identifier;
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
    sccImpl = new SpillableComplexComponentImpl(store);
    sccImpl.setup(context);
    internMap = sccImpl.newSpillableByteMap(identifier.getBytes(), bucket, windowSerde, valueSerde);
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
