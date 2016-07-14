package org.apache.apex.malhar.lib.window.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteMapImpl;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedKeyedStorage;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.Slice;

/**
 * This implement only keep as one bucket
 * @author bright
 *
 * @param <K>
 * @param <V>
 */
public abstract class SpillableWindowedKeyedStorageWithOneBucket<K, V, M extends Map<K, V>> implements WindowedKeyedStorage<K, V, M>
{
  @NotNull
  protected ManagedStateSpillableStateStore store;
  
  protected Serde<Window, Slice> serdeWindow;
  protected Serde<M, Slice> serdeMap;
  
  protected SpillableByteArrayListMultimapImpl<Long, Window> streamToEventWindow;
  protected SpillableByteMapImpl<Window, M> internMap;
  
  protected long currentWindowId;
  
  private SpillableWindowedKeyedStorageWithOneBucket(){}
  
  public SpillableWindowedKeyedStorageWithOneBucket(long bucket, Serde<Window, Slice> serdeWindow, Serde<M, Slice> serdeMap)
  {
    store = new ManagedStateSpillableStateStore();
    store.getCheckpointManager().setNeedBucketFile(false);
    //identifier can be used to identify the map if multiple map share one bucket.
    internMap = new SpillableByteMapImpl(store, new byte[0], bucket, serdeWindow, serdeMap);
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
  public void put(Window window, M value)
  {
    internMap.put(window, value);
  }

  @Override
  public M get(Window window)
  {
    return internMap.get(window);
  }

  @Override
  public Iterable<Entry<Window, M>> entrySet()
  {
    return internMap.entrySet();
  }

  @Override
  public void setup(OperatorContext context)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.currentWindowId = windowId;
  }
  
  @Override
  public void endWindow()
  {
    internMap.endWindow();
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    internMap.endWindow();
  }

  @Override
  public void committed(long windowId)
  {
  }

  @Override
  public Iterator<Entry<Window, M>> iterator()
  {
    return internMap.entrySet().iterator();
  }

  @Override
  public void put(Window window, K key, V value)
  {
    M valueMap = internMap.get(window);
    if(valueMap == null){
      valueMap = createMapForWindow(window);
      internMap.put(window, valueMap);
    }
    valueMap.put(key, value);
  }
  
  protected abstract M createMapForWindow(Window window);


  @Override
  public Iterable<Entry<K, V>> entrySet(Window window)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public V get(Window window, K key)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void remove(Window window)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void migrateWindow(Window fromWindow, Window toWindow)
  {
    // TODO Auto-generated method stub
    
  }

}
