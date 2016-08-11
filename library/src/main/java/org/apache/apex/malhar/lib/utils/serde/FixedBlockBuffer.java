package org.apache.apex.malhar.lib.utils.serde;

import com.datatorrent.netlet.util.Slice;

/**
 * This implementation will not allocate new memory if already output slices 
 *
 */
public class FixedBlockBuffer extends BlockBuffer
{
  public static class OutOfBlockBufferMemoryException extends RuntimeException
  {
    private static final long serialVersionUID = 3813792889200989131L;
  }
  
  
  public FixedBlockBuffer()
  {
    super();
  }
  
  public FixedBlockBuffer(int capacity)
  {
    super(capacity);
  }
  
  @Override
  public void write(byte[] data, final int offset, final int length) throws OutOfBlockBufferMemoryException
  {
    super.write(data, offset, length);
  }


  /**
   * check the buffer size and reallocate if buffer is not enough
   * @param length
   */
  @Override
  protected void checkOrReallocateBuffer(int length) throws OutOfBlockBufferMemoryException
  {
    if(size + length > capacity && slices.size() > 0) {
      throw new OutOfBlockBufferMemoryException();
    }
    
    super.checkOrReallocateBuffer(length);
  }
  
  /**
   * Similar as toSlice, this method is used to get the information of the object regards the data already write to buffer.
   * But unlike toSlice() which indicate all data of this object already done, this method can be called at any time
   */
  public Slice getLastObjectSlice()
  {
    return new Slice(buffer, objectBeginOffset, size - objectBeginOffset);
  }
  
  public void discardLastObjectData()
  {
    if(objectBeginOffset == 0)
      return;
    size = objectBeginOffset;
    objectBeginOffset = 0;
  }
  
  public void moveLastObjectDataTo(FixedBlockBuffer newBuffer)
  {
    newBuffer.write(buffer, objectBeginOffset, size - objectBeginOffset);
    discardLastObjectData();
  }
}
