package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 6/12/16.
 */
public class PassThruByteArraySliceSerde implements Serde<byte[], Slice>
{
  @Override
  public Slice serialize(byte[] object)
  {
    return new Slice(object);
  }

  @Override
  public byte[] deserialize(Slice object, MutableInt offset)
  {
    offset.add(object.length);

    if (object.offset == 0) {
      return object.buffer;
    }

    byte[] bytes = new byte[object.length];
    System.arraycopy(object.buffer, object.offset, bytes, 0, object.length);
    return bytes;
  }

  @Override
  public byte[] deserialize(Slice object)
  {
    return deserialize(object, new MutableInt(0));
  }
}
