package org.apache.apex.malhar.lib.utils.serde;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Maps;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * try to get rid of memory slice and memory data copy 
 * Basically used by memory serialize
 *
 */
public class LVBuffer
{
  protected static final int DEFAULT_CAPACITY = 10000;
  
  ByteArrayOutputStream outputSteam = new ByteArrayOutputStream();
  
  protected int currentOffset = 0;
  protected Map<Integer, Integer> placeHolderIdentifierToValue = Maps.newHashMap();
  
  public LVBuffer()
  {
    this(DEFAULT_CAPACITY);
  }
  
  public LVBuffer(int capacity)
  {
    outputSteam = new ByteArrayOutputStream(capacity);
  }
  
  protected transient final byte[] tmpLengthAsBytes = new byte[4];
  protected transient final MutableInt tmpOffset = new MutableInt(0);
  public void setObjectLength(int length)
  {
    try {
      GPOUtils.serializeInt(length, tmpLengthAsBytes, new MutableInt(0));
      outputSteam.write(tmpLengthAsBytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
  protected final byte[] lengthPlaceHolder = new byte[]{0, 0, 0, 0};
  public int markPlaceHolderForLength()
  {
    try {
      int offset = outputSteam.size();
      outputSteam.write(lengthPlaceHolder);
      return offset;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
  }
  
  public int getSize()
  {
    return outputSteam.size();
  }
  
  /**
   * 
   * @param placeHolderId
   * @param length
   */
  public void setValueForLengthPlaceHolder(int placeHolderId, int length)
  {
    //don't convert to byte array now. just keep the information
    placeHolderIdentifierToValue.put(placeHolderId, length);
  }
  
  public Slice toSlice()
  {
    byte[] data = outputSteam.toByteArray();
    
    MutableInt offset = new MutableInt();
    for(Map.Entry<Integer, Integer> entry : placeHolderIdentifierToValue.entrySet()) {
      offset.setValue(entry.getKey());
      GPOUtils.serializeInt(entry.getValue(), data, offset);
    }
    return new Slice(data, 0, data.length);
  }
  
  //reset environment for next object
  public void reset()
  {
    outputSteam.reset();
    placeHolderIdentifierToValue.clear();
  }
}