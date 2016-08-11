package org.apache.apex.malhar.lib.utils.serde;

import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.netlet.util.Slice;

/**
 * A stream implemented by an array of block ( or a map from index to block )
 * BlockBuffer can reallocate the memory and copy the data if buffer is not enough
 * But it is not the good solution if there already have slices output.
 * BlockStream try to avoid copy the data which already output slices
 * 
 */
public class BlockStream implements BufferStream
{
  public static final int DEFAULT_BLOCK_SIZE = 1000000;
  
  //the initial size of each block
  protected final int blockSize;
  
  protected Map<Integer, FixedBlockBuffer> blocks = Maps.newHashMap();
  //the index of current block
  protected int blockIndex = 0;
  protected int size = 0;
  
  protected FixedBlockBuffer currentBlock;
  
  public BlockStream()
  {
    this(DEFAULT_BLOCK_SIZE);
  }
  
  public BlockStream(int blockSize)
  {
    this.blockSize = blockSize;
  }
  
  @Override
  public void write(byte[] data)
  {
    write(data, 0, data.length);
  }
  
  /**
   * This write could be the first or the continuous write for an object.
   * For t
   * @param data
   * @param offset
   * @param length
   */
  @Override
  public void write(byte[] data, final int offset, final int length)
  {
    //start with a block which at least can hold current data
    currentBlock = getOrCreateCurrentBlock();
    
    try {
      currentBlock.write(data, offset, length);
    } catch (FixedBlockBuffer.OutOfBlockBufferMemoryException e) {
      //use next block
      FixedBlockBuffer newWorkBlock = moveToNextBlock();
      currentBlock.moveLastObjectDataTo(newWorkBlock);
      newWorkBlock.write(data, offset, length);
      currentBlock = newWorkBlock;
    }
  }
  
  protected FixedBlockBuffer moveToNextBlock()
  {
    ++blockIndex;
    return getOrCreateCurrentBlock();
  }
  
  protected FixedBlockBuffer getOrCreateCurrentBlock()
  {
    FixedBlockBuffer block = blocks.get(blockIndex);
    if(block == null) {
      block = new FixedBlockBuffer(blockSize);
      blocks.put(blockIndex, block);
    }
    return block;
  }
  
  @Override
  public int size()
  {
    return size;
  }
  
  /**
   * 
   * this is the last call which represent the end of an object
   */
  @Override
  public Slice toSlice()
  {
    return blocks.get(blockIndex).toSlice();
  }
  
  /**
   * Don't need to maintain original buffer now.
   */
  @Override
  public void reset()
  {
    blockIndex = 0;
    size = 0;
    for(FixedBlockBuffer block : blocks.values()) {
      block.reset();
    }
  }
  
}
