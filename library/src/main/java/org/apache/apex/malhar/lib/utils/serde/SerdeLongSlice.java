package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

public class SerdeLongSlice implements Serde<Long, Slice>
{
  @Override
  public Slice serialize(Long object)
  {
    return new Slice(GPOUtils.serializeLong(object));
  }

  @Override
  public Long deserialize(Slice slice, MutableInt offset)
  {
    return GPOUtils.deserializeLong(slice.buffer, offset);
  }

  @Override
  public Long deserialize(Slice object)
  {
    return deserialize(object, new MutableInt(0));
  }
}

