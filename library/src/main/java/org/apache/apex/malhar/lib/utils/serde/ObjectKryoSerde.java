package org.apache.apex.malhar.lib.utils.serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.commons.lang3.mutable.MutableInt;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.netlet.util.Slice;

/**
 * use Kryo to serialize/deserialize the object
 *
 * @param <T>
 */
public class ObjectKryoSerde<T> implements Serde<T, Slice>
{
  protected Class<T> clazz;
  protected Kryo kryo = new Kryo();

  public ObjectKryoSerde(Class<T> clazz)
  {
    this.clazz = clazz;
  }
  
  @Override
  public Slice serialize(T object)
  {
    ByteArrayOutputStream outputSteam = new ByteArrayOutputStream();
    Output kryoOutput = new Output(outputSteam);

    kryo.writeObject(kryoOutput, object);
    kryoOutput.close();
    return new Slice(outputSteam.toByteArray());
  }

  @Override
  public T deserialize(Slice slice, MutableInt offset)
  {
    return deserialize(slice);
  }

  @Override
  public T deserialize(Slice slice)
  {
    ByteArrayInputStream inputSteam = new ByteArrayInputStream(slice.buffer, slice.offset, slice.length);
    Input kryoInput = new Input(inputSteam);
    
    T object = kryo.readObject(kryoInput, clazz);
    kryoInput.close();
    return object;
  }

}
