package org.apache.apex.malhar.lib.window.impl;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.Spillable.SpillableByteMap;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteMapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedKeyedStorage;

import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.Slice;

/**
 * Convert input map into SpillableMap, and let the SpillableMap handle the storage.
 * 
 * Storage management: 
 *   1. each event window for one bucket. 
 *     pro: easy for event window related operation. such as remove( remove whole bucket ). 
 *     cons: not easy/heavy for streaming window related operation 
 *       could have lots of buckets and each bucket file could be small 
 *   2. each streaming window for one bucket
 *     pro: easy to handle streaming window related operation, such as checkpoint, committed
 *       easier to allocated buckets, as buckets was assigned as fixed array. The (currentWindowId - committedWindowId) can roughly as bucket size
 *     cons: 
 *
 * This implementation try to create one bucket for each event window. But the problem is we don't know how much bucket should be assigned.
 * so, if different bucketId targeted to the same bucket( buckets managed as array ). It would be a problem. 
 * 
 * @param <K>
 * @param <V>
 */
public class SpillableWindowedKeyedStorageBucketWithEventWindow<K, V> implements WindowedKeyedStorage<K, V, Spillable.SpillableByteMap<K, V>>
{
  protected SpillableByteMapImpl<Window, Spillable.SpillableByteMap<K, V>> internMap;
  
  @NotNull
  protected SpillableStateStore store;

  @NotNull
  protected Serde<K, Slice> serdeKey;
  @NotNull
  protected Serde<V, Slice> serdeValue;
  
  //protected long bucket;
  
  //windows ordered by end time asc
  protected TreeSet<Window> windowsOrderByEndTime = Sets.newTreeSet(new Comparator<Window>()
  {
    @Override
    public int compare(Window o1, Window o2)
    {
      return ( o1.getBeginTimestamp() + o1.getDurationMillis() < o2.getBeginTimestamp() + o2.getDurationMillis() ) ? -1 : 1;
    }
  });
  
  private SpillableWindowedKeyedStorageBucketWithEventWindow()
  {
    //for kryo
  }
  
  /**
   * Notes: need a serialize which generic enough to handle all type of window
   * @param identifier
   * @param bucket
   * @param serdeKey
   * @param serdeValue
   */
  public SpillableWindowedKeyedStorageBucketWithEventWindow(byte[] identifier, long bucket, Serde<K, Slice> serdeKey, Serde<V, Slice> serdeValue)
  {
    ManagedStateSpillableStateStore store = new ManagedStateSpillableStateStore();
    store.getCheckpointManager().setNeedBucketFile(false);
    this.store = store;
    this.serdeKey = serdeKey;
    this.serdeValue = serdeValue;
    internMap = new SpillableByteMapImpl(store, identifier, bucket, serdeKey, serdeValue);
  }
  
  public SpillableWindowedKeyedStorageBucketWithEventWindow(SpillableStateStore store, byte[] identifier, long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    this.store = store;
    //this.bucket = bucket;
    this.serdeKey = serdeKey;
    this.serdeValue = serdeValue;
    internMap = new SpillableByteMapImpl(store, identifier, bucket, serdeKey, serdeValue);
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
  public void put(Window window, Spillable.SpillableByteMap<K, V> value)
  {
    if(value == null || value.isEmpty())
      return;
    
    windowsOrderByEndTime.add(window);
    
    Spillable.SpillableByteMap<K, V> valueMap = internMap.get(window);
    if(valueMap == null){
      valueMap = createSpillableMapForWindow(window);
      internMap.put(window, valueMap);
    } else if(valueMap == value) {
      //it is possible the client get and then put, in this case, nothing need to do
      return;
    }
    
    for(Map.Entry<K, V> entry : value.entrySet()){
      valueMap.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Spillable.SpillableByteMap<K, V> get(Window window)
  {
    return internMap.get(window);
  }
  
  /**
   * create empty one if not exist;
   */
  public Spillable.SpillableByteMap<K, V> getOrCreate(Window window)
  {
    Spillable.SpillableByteMap<K, V> valueMap = internMap.get(window);
    if(valueMap == null){
      valueMap = createSpillableMapForWindow(window);
      internMap.put(window, valueMap);
    }
    return valueMap;
  }

  @Override
  public Iterable<Entry<Window, Spillable.SpillableByteMap<K, V>>> entrySet()
  {
    return internMap.entrySet();

  }

  @Override
  public void put(Window window, K key, V value)
  {
    windowsOrderByEndTime.add(window);
    
    Spillable.SpillableByteMap<K, V> valueMap = internMap.get(window);
    if(valueMap == null){
      valueMap = createSpillableMapForWindow(window);
      internMap.put(window, valueMap);
    }
    valueMap.put(key, value);
  }

  @Override
  public Iterable<Entry<K, V>> entrySet(Window window)
  {
    return internMap.get(window).entrySet();
  }


  @Override
  public Iterator<Entry<Window, SpillableByteMap<K, V>>> iterator()
  {
    return internMap.entrySet().iterator();
  }
  
//  @Override
//  public Set<Window> windowsEndBefore(long timestamp)
//  {
//    return windowsOrderByEndTime.headSet(new Window.TimeWindow(0, timestamp));
//  }

  @Override
  public V get(Window window, K key)
  {
    Spillable.SpillableByteMap<K, V> valueMap = internMap.get(window);
    return valueMap == null ? null : valueMap.get(key);
  }

  /**
   * After received watermark, it is not expected to receive the tuples of this window very often.
   * call this method to move data from memory to storage.
   */
  public void passivate()
  {
    
  }
  
  @Override
  public void remove(Window window)
  {
    internMap.remove(window);
  }

//  @Override
//  public void removeUpTo(long timestamp)
//  {
//    Set<Window> windows = windowsEndBefore(timestamp);
//    for(Window window : windows) {
//      internMap.remove(window);
//    }
//  }

  @Override
  public void migrateWindow(Window fromWindow, Window toWindow)
  {
    // TODO Auto-generated method stub
    
  }

 
  /**
   * FIXME: not allowed to create multiple with 
   * @param window
   * @return
   */
  public Spillable.SpillableByteMap<K, V> createSpillableMapForWindow(Window window)
  {
    //TODO: how to create identifier and bucket id?
    //The whole window will be removed together, maybe whole window share one bucket
    //bucket id must > 0;
    long bucket = ((window.getBeginTimestamp() & 0x7fffffff) << 32) + (window.getDurationMillis() & 0xffffffff);
    
    final int longBytes = Long.SIZE/Byte.SIZE;
    
    ByteBuffer buffer = ByteBuffer.allocate(longBytes * 2);
    buffer.putLong(window.getBeginTimestamp());
    buffer.putLong(window.getDurationMillis());
    byte[] identifier = buffer.array();
    
    return new SpillableByteMapImpl(store, identifier, bucket, serdeKey, serdeValue);
  }
  

  public void setup(OperatorContext context)
  {
    store.setup(context);
  }

  public void beginWindow(long windowId)
  {
  }
  
  /**
   * don't call internMap.endWindow() now as storage will save the data to file until beforeCheckpoint().
   * call internMap.endWindow() will keep two copy of data in memory (another one is serialized format)
   */
  public void endWindow()
  {
    //The SpillableByteMapImpl.endWindow() sync memory with storage. and remove the LRU entry from memory.
    //For deleted entry, put empty instead of remove it
    internMap.endWindow();
  }
  
  /**
   * the storage will save the data to file until beforeCheckpoint().
   * so, do the serialize until now
   */
  public void beforeCheckpoint(long windowId)
  {
    internMap.endWindow();
    store.beforeCheckpoint(windowId);
  }

  /**
   * NOTE: The default implementation of storage will transfer the file.
   * But we should remove the window files in this case.
   * 
   */
  public void committed(long windowId)
  {
    
  }
}
