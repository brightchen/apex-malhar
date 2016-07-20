package org.apache.apex.malhar.lib.utils.serde;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.netlet.util.Slice;

/**
 * try to get rid of memory slice and memory data copy 
 * Basically used by memory serialize
 *
 */
public class LVBuffer
{
  protected static final int DEFAULT_CAPACITY = 10000;
  protected ByteBuffer byteBuffer;
  
  ByteArrayOutputStream outputSteam = new ByteArrayOutputStream();
  
  protected int currentOffset = 0;
  
  public LVBuffer()
  {
    this(DEFAULT_CAPACITY);
  }
  
  public LVBuffer(int capacity)
  {
    byteBuffer = ByteBuffer.allocate(capacity);
  }
  
  public void setObjectLength(int length)
  {
    outputSteam.write(length);
  }
  
  /**
   * only set value.
   * @param value
   * @param offset
   * @param length
   */
  public void setObjectValue(byte[] value, int offset, int length)
  {
    outputSteam.write(value, offset, length);
  }
  
  /**
   * set value and length. the input value is value only, it doesn't include length information.
   * @param value
   * @param offset
   * @param length
   */
  public void setObjectWithValue(byte[] value, int offset, int length)
  {
    setObjectLength(length);
    setObjectValue(value, offset, length);
  }
  
  public void setObjectWithValue(byte[] value)
  {
    setObjectWithValue(value, 0, value.length);
  }

  /**
   * mark place hold for length.
   * In some case, we don't know the length until really processed data.
   * mark place holder for set length later.
   * 
   * @return the identity for this placeholder
   */
  public long markPlaceHolderForLength()
  {
    
  }
  
  /**
   * 
   * @param placeHolderId
   * @param length
   */
  public void setValueForLengthPlaceHolder(long placeHolderId, int length)
  {
    
  }
  
  public Slice toSlice()
  {
    return new Slice(outputSteam.toByteArray());
  }
}