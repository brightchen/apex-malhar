package org.apache.apex.malhar.lib.window.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.validation.constraints.NotNull;

import java.util.Set;


import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.Spillable.SpillableByteMap;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteMapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedKeyedStorage;

import com.google.common.collect.Maps;

import com.datatorrent.netlet.util.Slice;

/**
 * Convert input map into SpillableMap, and let the SpillableMap handle the storage.
 * It should be the client code directly use SpillableMap
 * 
 *
 * @param <K>
 * @param <V>
 */
public class SpillableWindowedKeyedStorage<K, V> implements WindowedKeyedStorage<K, V, Spillable.SpillableByteMap<K, V>>
{
  protected Spillable.SpillableByteMap<Window, Spillable.SpillableByteMap<K, V>> internMap;
  
  @NotNull
  private SpillableStateStore store;

  @NotNull
  private Serde<K, Slice> serdeKey;
  @NotNull
  private Serde<V, Slice> serdeValue;
  
  public SpillableWindowedKeyedStorage(SpillableStateStore store, byte[] identifier, long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    this.store = store;
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
    
    Spillable.SpillableByteMap<K, V> valueMap = internMap.get(window);
    if(valueMap == null){
      valueMap = createSpillableMap();
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

  @Override
  public Iterable<Entry<Window, Spillable.SpillableByteMap<K, V>>> entrySet()
  {
    return internMap.entrySet();

  }

  @Override
  public void put(Window window, K key, V value)
  {

    Spillable.SpillableByteMap<K, V> valueMap = internMap.get(window);
    if(valueMap == null){
      valueMap = createSpillableMap();
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
  
  @Override
  public Set<Window> windowsEndBefore(long timestamp)
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
  public void removeUpTo(long timestamp)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void migrateWindow(Window fromWindow, Window toWindow)
  {
    // TODO Auto-generated method stub
    
  }


}
