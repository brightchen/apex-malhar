package org.apache.apex.malhar.lib.state.spillable;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class WindowBoundedMapCache<K, V>
{
  public static final int DEFAULT_MAX_SIZE = 50000;

  private int maxSize = DEFAULT_MAX_SIZE;

  private Map<K, V> cache = Maps.newHashMap();

  private Set<K> changedKeys = Sets.newHashSet();
  private Set<K> removedKeys = Sets.newHashSet();
  private TimeBasedPriorityQueue<K> priorityQueue = new TimeBasedPriorityQueue<>();

  public WindowBoundedMapCache()
  {
  }

  public WindowBoundedMapCache(int maxSize)
  {
    Preconditions.checkArgument(maxSize > 0);

    this.maxSize = maxSize;
  }

  public void put(K key, V value)
  {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);

    removedKeys.remove(key);
    changedKeys.add(key);
    priorityQueue.upSert(key);

    cache.put(key, value);
  }

  public V get(K key)
  {
    Preconditions.checkNotNull(key);

    return cache.get(key);
  }

  public boolean contains(K key)
  {
    return cache.containsKey(key);
  }

  public void remove(K key)
  {
    Preconditions.checkNotNull(key);

    if (!cache.containsKey(key)) {
      return;
    }

    cache.remove(key);
    changedKeys.remove(key);
    removedKeys.add(key);
    priorityQueue.remove(key);
  }

  public Set<K> getChangedKeys()
  {
    return changedKeys;
  }

  public Set<K> getRemovedKeys()
  {
    return removedKeys;
  }

  /*
    Note: beginWindow is intentionally not implemented because many users need a cache that does not require
    beginWindow to be called.
   */

  public void endWindow()
  {
    int count = cache.size() - maxSize;

    if (count > 0) {
      Set<K> expiredKeys = priorityQueue.removeLRU(count);

      for (K expiredKey: expiredKeys) {
        cache.remove(expiredKey);
      }
    }

    changedKeys = Sets.newHashSet();
    removedKeys = Sets.newHashSet();
  }
}
