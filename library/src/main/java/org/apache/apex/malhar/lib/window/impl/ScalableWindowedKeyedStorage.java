package org.apache.apex.malhar.lib.window.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.apex.malhar.lib.state.managed.ManagedStateImpl;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedKeyedStorage;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.netlet.util.Slice;

/**
 * The storing of map can be implemented by store whole map as one value 
 * or each entry as key value.
 * 
 * if store each entry of the map, it would be difficult to get by window as we don't the keys of map.
 * 
 * so, keep whole map as a value. the short coming is it must load/save whole map and affect the performance.
 * 
 */
public class ScalableWindowedKeyedStorage<K, V> extends ManagedStateImpl implements WindowedKeyedStorage<K, V>
{
  //keep the set of window for iterate
  protected Set<Window> windows = Sets.newHashSet();
  protected Serde<Map<K, V>, Slice> serde;
  
  @Override
  public boolean containsWindow(Window window)
  {
    return windows.contains(window);
  }

  @Override
  public long size()
  {
    return windows.size();
  }

  /**
   * Very heavy: need to get old data and merge and store
   * @param window
   * @param value
   */
  @Override
  public void put(Window window, Map<K, V> value)
  {
    if(value == null || value.isEmpty())
      return;
    
    Map<K, V> storedValue = null;
    if( windows.contains(window)){
      storedValue = get(window);
    } else {
      windows.add(window);
      storedValue = Maps.newHashMap();
    }
    
    storedValue = merge(storedValue, value);
    
    final long bucketId = getBucketId(window);
    Slice valueSer = serde.serialize(storedValue);
    
    //need to remove the old value?
    super.put(bucketId, getKey(window), valueSer);
  }

  protected Map<K, V> merge(Map<K, V> storedValue, Map<K, V> newValue)
  {
    for (Map.Entry<K, V> newValueEntry : newValue.entrySet()) {
      storedValue.put(newValueEntry.getKey(), newValueEntry.getValue());
    }
    return storedValue;
  }
  
//  public void serialize(Map<K, V> value)
//  {
//    int offset = 0;
//    byte[] ba = null;
//    ByteArrayOutputStream b = new ByteArrayOutputStream();
//    for (Map.Entry<K, V> entry : value.entrySet()) {
//      ObjectOutputStream o = new ObjectOutputStream(b);
//      o.writeObject(entry.getKey());
//      ba = b.toByteArray();
//      Slice keySlice = new Slice(ba, offset, ba.length - offset);
//      offset += ba.length;
//      
//      o.writeObject(entry.getValue());
//      ba = b.toByteArray();
//      Slice valueSlice = new Slice(ba, offset, ba.length - offset);
//      super.put(bucketId, keySlice, valueSlice);
//    }
//  }
  
  
  /**
   * Very heavy: get all map. but most of data may useless at all
   * @param window
   * @return
   */
  @Override
  public Map<K, V> get(Window window)
  {
    return serde.deserialize(super.getSync(this.getBucketId(window), this.getKey(window)));
  }

  @Override
  public Iterable<Entry<Window, Map<K, V>>> entrySet()
  {
    //load all value for iterate could be very have. should provide lazy loading.
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Entry<Window, Map<K, V>>> iterator()
  {
    //load all value for iterate could be very have. should provide lazy loading.
    throw new UnsupportedOperationException();
  }

  @Override
  public void put(Window window, K key, V value)
  {
    //Very Heavy: need  to load whole map, merge and then save
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<Entry<K, V>> entrySet(Window window)
  {
    //Very Heavy: need to load whole map, merge and then save
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Window> windowsEndBefore(long timestamp)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public V get(Window window, K key)
  {
    //Very Heavy: need to load whole map, and then return the value
    throw new UnsupportedOperationException();
  }

  @Override
  public void remove(Window window)
  {
    //Very Heavy: need to load whole map, remove and then save
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeUpTo(long timestamp)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void migrateWindow(Window fromWindow, Window toWindow)
  {
    // TODO Auto-generated method stub
    
  }

  
  /**
   * get the bucket from window
   * @param window
   * @return
   */
  public long getBucketId(Window window)
  {
    return window.getBeginTimestamp() << 32 + window.getDurationMillis();
  }
  
  public Slice getKey(Window window)
  {
    try {
      ByteArrayOutputStream b = new ByteArrayOutputStream();
      ObjectOutputStream o = new ObjectOutputStream(b);
      o.writeLong(window.getBeginTimestamp());
      o.writeLong(window.getDurationMillis());
      return new Slice(b.toByteArray());
    } catch (IOException e) {
      //ignore
      return null;
    }
  }
}
