package org.apache.apex.malhar.lib.state.spillable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.utils.serde.PassThruByteArraySliceSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerdeIntSlice;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 6/12/16.
 */
@DefaultSerializer(FieldSerializer.class)
public class SpillableByteArrayListMultimapImpl<K, V> implements Spillable.SpillableByteArrayListMultimap<K, V>,
    Spillable.SpillableComponent
{
  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final byte[] SIZE_KEY_SUFFIX = new byte[]{(byte)0, (byte)0, (byte)0};

  private int batchSize = DEFAULT_BATCH_SIZE;

  private WindowBoundedMapCache<K, SpillableArrayListImpl<V>> cache = new WindowBoundedMapCache<>();

  @NotNull
  private SpillableByteMapImpl<byte[], Integer> map;

  private SpillableStateStore store;
  private byte[] identifier;
  private long bucket;
  private Serde<K, Slice> serdeKey;
  private Serde<V, Slice> serdeValue;

  private boolean isRunning = false;
  private boolean isInWindow = false;

  private SpillableByteArrayListMultimapImpl()
  {
    // for kryo
  }

  public SpillableByteArrayListMultimapImpl(SpillableStateStore store, byte[] identifier, long bucket,
      Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    this.store = Preconditions.checkNotNull(store);
    this.identifier = Preconditions.checkNotNull(identifier);
    this.bucket = bucket;
    this.serdeKey = Preconditions.checkNotNull(serdeKey);
    this.serdeValue = Preconditions.checkNotNull(serdeValue);

    map = new SpillableByteMapImpl(store, identifier, bucket, new PassThruByteArraySliceSerde(), new SerdeIntSlice());
  }

  @Override
  public List<V> get(@Nullable K key)
  {
    return getHelper(key);
  }

  private SpillableArrayListImpl<V> getHelper(@Nullable K key)
  {
    SpillableArrayListImpl<V> spillableArrayList = cache.get(key);

    if (spillableArrayList == null) {
      Slice keyPrefix = SliceUtils.concatenate(identifier, serdeKey.serialize(key));
      Integer size = map.get(SliceUtils.concatenate(keyPrefix, SIZE_KEY_SUFFIX).toByteArray());

      if (size == null) {
        return null;
      }

      spillableArrayList = new SpillableArrayListImpl<V>(bucket, keyPrefix.toByteArray(), store, serdeValue);
      spillableArrayList.setSize(size);
    }

    cache.put(key, spillableArrayList);

    return spillableArrayList;
  }

  @Override
  public Set<K> keySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Multiset<K> keys()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<V> values()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<Map.Entry<K, V>> entries()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<V> removeAll(@Nullable Object key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size()
  {
    return map.size();
  }

  @Override
  public boolean isEmpty()
  {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(@Nullable Object key)
  {
    return map.containsKey(SliceUtils.concatenate(serdeKey.serialize((K)key).toByteArray(), SIZE_KEY_SUFFIX));
  }

  @Override
  public boolean containsValue(@Nullable Object value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsEntry(@Nullable Object key, @Nullable Object value)
  {
    if (!containsKey(key)) {
      return false;
    }
    List<V> values = get((K)key);
    //TODO: linear search is inefficient
    for (int i = 0; i < values.size(); i++) {
      V v = values.get(i);
      if (value == null) {
        if (v == null) {
          return true;
        }
      } else {
        if (value.equals(v)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean put(@Nullable K key, @Nullable V value)
  {
    SpillableArrayListImpl<V> spillableArrayList = getHelper(key);

    if (spillableArrayList == null) {
      Slice keyPrefix = SliceUtils.concatenate(identifier, serdeKey.serialize(key));
      spillableArrayList = new SpillableArrayListImpl<V>(bucket, keyPrefix.toByteArray(), store, serdeValue);

      cache.put(key, spillableArrayList);
    }

    spillableArrayList.add(value);
    return true;
  }

  @Override
  public boolean remove(@Nullable Object key, @Nullable Object value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean putAll(@Nullable K key, Iterable<? extends V> values)
  {
    boolean changed = false;

    for (V value: values) {
      changed |= put(key, value);
    }

    return changed;
  }

  @Override
  public boolean putAll(Multimap<? extends K, ? extends V> multimap)
  {
    boolean changed = false;

    for (Map.Entry<? extends K, ? extends V> entry: multimap.entries()) {
      changed |= put(entry.getKey(), entry.getValue());
    }

    return changed;
  }

  @Override
  public List<V> replaceValues(K key, Iterable<? extends V> values)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<K, Collection<V>> asMap()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    map.setup(context);
    isRunning = true;
  }

  @Override
  public void beginWindow(long windowId)
  {
    map.beginWindow(windowId);
    isInWindow = true;
  }

  @Override
  public void endWindow()
  {
    isInWindow = false;
    for (K key: cache.getChangedKeys()) {

      SpillableArrayListImpl<V> spillableArrayList = cache.get(key);
      spillableArrayList.endWindow();

      Integer size = map.put(SliceUtils.concatenate(serdeKey.serialize(key), SIZE_KEY_SUFFIX).toByteArray(),
          spillableArrayList.size());
    }

    Preconditions.checkState(cache.getRemovedKeys().isEmpty());
    cache.endWindow();
    map.endWindow();
  }

  @Override
  public void teardown()
  {
    isRunning = false;
    map.teardown();
  }

  @Override
  public boolean isRunning()
  {
    return isRunning;
  }

  @Override
  public boolean isInWindow()
  {
    return isInWindow;
  }
}
