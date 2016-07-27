package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 7/22/16.
 */
public class PassThruSliceSerde implements Serde<Slice, Slice>
{
  @Override
  public Slice serialize(Slice object)
  {
    return object;
  }

  @Override
  public Slice deserialize(Slice object, MutableInt offset)
  {
    return object;
  }

  @Override
  public Slice deserialize(Slice object)
  {
    return object;
  }
}
