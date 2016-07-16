package org.apache.apex.malhar.lib.window.impl;

import java.util.Iterator;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.SpillableByteMapImpl;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * This is an implementation of WindowedPlainStorage that makes use of Spillable data structures
 *
 * @param <T> Type of the value per window
 */
public class SpillableWindowedPlainStorage<T> implements WindowedStorage.WindowedPlainStorage<T>, Component<Context.OperatorContext>
{
  @NotNull
  private final ManagedStateSpillableStateStore store;

  protected final SpillableByteMapImpl<Window, T> internMap;

  private SpillableWindowedPlainStorage()
  {
    // for kryo
    store = null;
    internMap = null;
  }

  public SpillableWindowedPlainStorage(long bucket, byte[] identifier, Serde<Window, Slice> serdeWindow, Serde<T, Slice> serdeValue)
  {
    store = new ManagedStateSpillableStateStore();
    store.getCheckpointManager().setNeedBucketFile(false);
    internMap = new SpillableByteMapImpl<>(store, identifier, bucket, serdeWindow, serdeValue);
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
    store.setup(context);
    internMap.setup(context);
  }

  @Override
  public void teardown()
  {
    internMap.teardown();
    store.teardown();
  }

  @Override
  public void beginApexWindow(long windowId)
  {
    store.beginWindow(windowId);
  }

  @Override
  public void endApexWindow()
  {
    store.endWindow();
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
