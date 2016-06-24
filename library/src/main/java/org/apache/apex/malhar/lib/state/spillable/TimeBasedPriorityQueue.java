package org.apache.apex.malhar.lib.state.spillable;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TimeBasedPriorityQueue<T>
{
  private Map<T, TimeWrapper<T>> timeWrappperMap = Maps.newHashMap();
  private Set<TimeWrapper<T>> sortedTimestamp = Sets.newTreeSet();

  public void upSert(T value)
  {
    TimeWrapper<T> timeWrapper = timeWrappperMap.get(value);

    if (timeWrapper != null) {
      sortedTimestamp.remove(timeWrapper);
      timeWrapper.setTimestamp(System.currentTimeMillis());
    } else {
      timeWrapper = new TimeWrapper<>(value, System.currentTimeMillis());
      timeWrappperMap.put(value, timeWrapper);
    }

    sortedTimestamp.add(timeWrapper);
  }

  public void remove(T value)
  {
    TimeWrapper<T> timeWrapper = timeWrappperMap.get(value);
    sortedTimestamp.remove(timeWrapper);
    timeWrappperMap.remove(value);
  }

  public Set<T> removeLRU(int count)
  {
    Preconditions.checkArgument(count > 0 && count <= timeWrappperMap.size());

    Iterator<TimeWrapper<T>> iterator = sortedTimestamp.iterator();
    Set<T> valueSet = Sets.newHashSet();

    for (int counter = 0; counter < count; counter++) {
      T value = iterator.next().getKey();
      valueSet.add(value);
      timeWrappperMap.remove(value);
      iterator.remove();
    }

    return valueSet;
  }

  protected static class TimeWrapper<T> implements Comparable<TimeWrapper<T>>
  {
    private T key;
    private long timestamp;

    public TimeWrapper(T key, long timestamp)
    {
      this.key = Preconditions.checkNotNull(key);
      this.timestamp = timestamp;
    }

    public T getKey()
    {
      return key;
    }

    public long getTimestamp()
    {
      return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
      this.timestamp = timestamp;
    }

    @Override
    public int compareTo(TimeWrapper<T> timeWrapper)
    {
      if (this.timestamp < timeWrapper.getTimestamp()) {
        return -1;
      } else if (this.timestamp > timeWrapper.getTimestamp()) {
        return 1;
      }

      return 0;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TimeWrapper<?> that = (TimeWrapper<?>)o;

      return key.equals(that.key);
    }

    @Override
    public int hashCode()
    {
      return key.hashCode();
    }

    @Override
    public String toString()
    {
      return "TimeWrapper{" +
          "key=" + key +
          ", timestamp=" + timestamp +
          '}';
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TimeBasedPriorityQueue.class);
}
