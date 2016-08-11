package org.apache.apex.malhar.lib.utils.serde;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.netlet.util.Slice;

/**
 * 
 * keep the information of one block
 *
 */
public class BlockBuffer implements BufferStream
{
  public static transient final Logger logger = LoggerFactory.getLogger(BlockBuffer.class);
      
  public static final int DEFAULT_BLOCK_SIZE = 1000000;
  
  //the capacity of the block
  protected int capacity;
  
  //the size of the data.
  protected int size;
  
  protected int objectBeginOffset = 0;
  protected byte[] buffer;
  
  protected List<Slice> slices = Lists.newArrayList();
  
  public BlockBuffer()
  {
    this(DEFAULT_BLOCK_SIZE);
  }
  
  public BlockBuffer(int capacity)
  {
    if(capacity <= 0)
      throw new IllegalArgumentException("Invalid capacity: " + capacity);
    buffer = new byte[capacity];
    this.capacity = capacity;
  }
  
  /**
   * check the buffer size and reallocate if buffer is not enough
   * @param length
   */
  protected void checkOrReallocateBuffer(int length)
  {
    if(size + length <= capacity) {
      return;
    }
    
    //calculate the new capacity
    capacity = (size + length) * 2;
    
    logger.info("Going to assign buffer size: {}", capacity);
    
    byte[] oldBuffer = buffer;
    buffer = new byte[capacity];
    
    //NOTES: it's not a good idea to move the data after expose the slices. 
    //but if move the data, also need to change the exposed slices( we suppose client code will not cache the buffer reference
    if(size > 0) {
      System.arraycopy(oldBuffer, 0, buffer, 0, size);
      
      for(Slice slice : slices) {
        slice.buffer = buffer;
      }
    }
  }
  
  public void write(byte[] data)
  {
    write(data, 0, data.length);
  }
  
  public void write(byte[] data, final int offset, final int length)
  {
    checkOrReallocateBuffer(length);

    System.arraycopy(data, offset, buffer, size, length);
    size += length;
  }
  
  /**
   * the process of write an object should be:
   *  write(), write() ... write(), when write object done( before write another object), call toSlice();
   * @return
   */
  public Slice toSlice()
  {
    if(size == objectBeginOffset) {
      throw new RuntimeException("data size is zero.");
    }
    Slice slice = new Slice(buffer, objectBeginOffset, size - objectBeginOffset);
    slices.add(slice);
    //prepare for next object
    objectBeginOffset = size;
    return slice;
  }
  
  public void reset()
  {
    size = 0;
    slices.clear();
    objectBeginOffset = 0;
  }
  
  /**
   * check if has enough space for the length
   * @param length
   * @return
   */
  public boolean hasEnoughSpace(int length)
  {
    return size + length < capacity;
  }
  
  public int size()
  {
    return size;
  }
}
