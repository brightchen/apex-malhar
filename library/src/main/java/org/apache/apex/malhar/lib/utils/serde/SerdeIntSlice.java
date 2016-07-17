package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 6/12/16.
 */
public class SerdeIntSlice implements Serde<Integer, Slice>
{
  @Override
  public Slice serialize(Integer object)
  {
    return new Slice(GPOUtils.serializeInt(object));
  }

  @Override
  public Integer deserialize(Slice slice, MutableInt offset)
  {
    int val = GPOUtils.deserializeInt(slice.buffer, new MutableInt(slice.offset + offset.intValue()));
    offset.add(4);
    return val;
  }

  @Override
  public Integer deserialize(Slice object)
  {
    return deserialize(object, new MutableInt(0));
  }
}
