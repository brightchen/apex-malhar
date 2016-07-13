package org.apache.apex.malhar.lib.state.spillable.managed;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.AbstractSpillableComplexComponent;
import org.apache.apex.malhar.lib.utils.serde.Serde;

import com.google.common.base.Preconditions;

import com.datatorrent.netlet.util.Slice;

public class ManagedStateSpillableComplexComponent extends AbstractSpillableComplexComponent
{
  @NotNull
  private ManagedStateSpillableStateStore store;

  public ManagedStateSpillableComplexComponent(@NotNull ManagedStateSpillableStateStore store)
  {
    this.store = Preconditions.checkNotNull(store);
  }

  @Override
  public <T> SpillableArrayList<T> newSpillableArrayList(long bucket, Serde<T, Slice> serde)
  {
    return null;
  }

  @Override
  public <T> SpillableArrayList<T> newSpillableArrayList(byte[] identifier, long bucket,
      Serde<T, Slice> serde)
  {
    return null;
  }

  @Override
  public <K, V> SpillableByteMap<K, V> newSpillableByteMap(long bucket, Serde<K, Slice> serdeKey,
      Serde<V, Slice> serdeValue)
  {
    return null;
  }

  @Override
  public <K, V> SpillableByteMap<K, V> newSpillableByteMap(byte[] identifier, long bucket,
      Serde<K, Slice> serdeKey, Serde<V, Slice> serdeValue)
  {
    return null;
  }

  @Override
  public <K, V> SpillableByteArrayListMultimap<K, V> newSpillableByteArrayListMultimap(long bucket,
      Serde<K, Slice> serdeKey, Serde<V, Slice> serdeValue)
  {
    return null;
  }

  @Override
  public <K, V> SpillableByteArrayListMultimap<K, V> newSpillableByteArrayListMultimap(byte[] identifier,
      long bucket, Serde<K, Slice> serdeKey, Serde<V, Slice> serdeValue)
  {
    return null;
  }

  @Override
  public <T> SpillableByteMultiset<T> newSpillableByteMultiset(long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> SpillableByteMultiset<T> newSpillableByteMultiset(byte[] identifier, long bucket,
      Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> SpillableQueue<T> newSpillableQueue(long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> SpillableQueue<T> newSpillableQueue(byte[] identifier, long bucket, Serde<T, Slice> serde)
  {
    throw new UnsupportedOperationException();
  }
}
