package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableLong;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class MutableLongSerde implements Serde<MutableLong>
{
  @Override
  public void serialize(MutableLong value, Output output)
  {
    output.writeLong(value.longValue());
  }

  @Override
  public MutableLong deserialize(Input input)
  {
    return new MutableLong(input.readLong());
  }
}
