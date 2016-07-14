package org.apache.apex.malhar.lib.state.spillable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.apex.malhar.lib.state.spillable.Spillable.SpillableByteArrayListMultimap;
import org.apache.apex.malhar.lib.state.spillable.Spillable.SpillableByteMap;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.Pair;

/**
 * This class implementation SpillableMapOfMap by delegates value management to SpillableByteMap implementation
 * This implementation need to create lots of internal data.
 */
public class SpillableMapOfMapImpl<PK, SK, V> implements SpillableMapOfMap<PK, SK, V>
{
  /**
   * implement map of map via two maps;
   */
  protected SpillableByteMap<PK, SK> parentMap;
  protected SpillableByteMap<Pair<PK, SK>, V> subMap;
  

  public SpillableMapOfMapImpl()
  {
    
  }
  
  @Override
  public int size()
  {
    return subMap.size();
  }

  @Override
  public boolean isEmpty()
  {
    return subMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key)
  {
    return parentMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value)
  {
    return subMap.containsValue(value);
  }

  @Override
  public SpillableByteMap<SK, V> get(Object key)
  {
    //need to create temporary map and return. heavy
    //investigate if client really need it.
    return null;
  }

  @Override
  public SpillableByteMap<SK, V> put(PK key, SpillableByteMap<SK, V> value)
  {
    //need to handle each entry.
    //investigate if client really need it.
    return null;
  }

  @Override
  public SpillableByteMap<SK, V> remove(Object key)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void putAll(Map<? extends PK, ? extends SpillableByteMap<SK, V>> m)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void clear()
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Set<PK> keySet()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<SpillableByteMap<SK, V>> values()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<java.util.Map.Entry<PK, SpillableByteMap<SK, V>>> entrySet()
  {
    // TODO Auto-generated method stub
    return null;
  }

}
