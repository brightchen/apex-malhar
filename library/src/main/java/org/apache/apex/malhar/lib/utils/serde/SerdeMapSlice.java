package org.apache.apex.malhar.lib.utils.serde;

import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * 
 * current implementation each serialize allocate memory which create a lot of temp object and memory slice
 * should create an mechanism to avoid it
 *
 * @param <K>
 * @param <V>
 */
public class SerdeMapSlice<K, V> implements Serde<Map<K, V>, Slice>
{
  @NotNull
  protected Serde<K, Slice> keySerde;
  @NotNull 
  protected Serde<V, Slice> valueSerde;

  public SerdeMapSlice(@NotNull Serde<K, Slice> keySerde, @NotNull Serde<V, Slice> valueSerde)
  {
    this.keySerde = Preconditions.checkNotNull(keySerde);
    this.valueSerde = Preconditions.checkNotNull(valueSerde);
  }

  @Override
  public Slice serialize(Map<K, V> map)
  {
    Slice[] slices = new Slice[map.size()];

    final int lengthSize = Integer.SIZE/Byte.SIZE;
    int totalSize = 0;
    int index = 0;
    for(Map.Entry<K, V> entry : map.entrySet()) {
      Slice keySlice = keySerde.serialize(entry.getKey());
      Slice valueSlice = valueSerde.serialize(entry.getValue());
      slices[index] = SliceUtils.concatenate(keySlice, valueSlice);
      //one entry format: length key value
      totalSize += (slices[index].length + lengthSize);
    }
    
    byte[] bytes = new byte[totalSize+4];
    int offset = 0;

    byte[] sizeBytes = GPOUtils.serializeInt(map.size());
    System.arraycopy(sizeBytes, 0, bytes, offset, 4);
    offset += 4;

    for (index = 0; index < slices.length; index++) {
      Slice slice = slices[index];
      System.arraycopy(slice.buffer, slice.offset, bytes, offset, slice.length);
      offset += slice.length;
    }

    return new Slice(bytes);
  }

  @Override
  public Map<K, V> deserialize(Slice slice, MutableInt offset)
  {
    MutableInt sliceOffset = new MutableInt(slice.offset + offset.intValue());

    int numElements = GPOUtils.deserializeInt(slice.buffer, sliceOffset);
    Map<K, V> map = createEmptyMap();
    sliceOffset.subtract(slice.offset);

    for (int index = 0; index < numElements; index++) {
      K key = keySerde.deserialize(slice, sliceOffset);
      V value = valueSerde.deserialize(slice, sliceOffset);
      map.put(key, value);
    }

    offset.setValue(sliceOffset.intValue());
    return map;
  }

  @Override
  public Map<K, V> deserialize(Slice slice)
  {
    return deserialize(slice, new MutableInt(0));
  }
  
  protected Map<K, V> createEmptyMap()
  {
    return Maps.newHashMap();
  }
}