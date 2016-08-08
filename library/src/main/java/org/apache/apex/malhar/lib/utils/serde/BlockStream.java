package org.apache.apex.malhar.lib.utils.serde;

import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.netlet.util.Slice;

/**
 * A stream implemented by an array of block ( or a map from index to block )
 *
 */
public class BlockStream
{
  public static final int DEFAULT_BLOCK_SIZE = 64-000-000;
  
  //the initial size of each block
  protected final int blockSize;
  
  protected Map<Integer, BlockBuffer> blocks = Maps.newHashMap();
  //the index of current block
  protected int blockIndex = -1;
  
  //each writing should only limit in one block.
  //the begin offset for current writing
  protected int objectBeginOffset = 0;
  //the length for current writing
  protected int objectLength = 0;
  protected int size = 0;
  
  public BlockStream()
  {
    this(DEFAULT_BLOCK_SIZE);
  }
  
  public BlockStream(int blockSize)
  {
    this.blockSize = blockSize;
  }
  
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
  public void write(byte[] data, final int offset, final int length)
  {
    if(length > blockSize) {
      throw new IllegalArgumentException("The data length is large than the block size. data length: " + length + "; block size: " + blockSize);
    }
    if(blockIndex < 0) {
      blockIndex = 0;
    }
    
    //start with a block which at least can hold current data
    BlockBuffer workBlock = getOrCreateCurrentBlock();
    if(!workBlock.hasEnoughSpace(length)) {
      workBlock = moveToNextBlock();
    }
    workBlock.write(data, offset, length);
  }
  
  protected BlockBuffer moveToNextBlock()
  {
    ++blockIndex;
    return getOrCreateCurrentBlock();
  }
  
  protected BlockBuffer getOrCreateCurrentBlock()
  {
    BlockBuffer block = blocks.get(blockIndex);
    if(block == null) {
      block = new BlockBuffer(blockSize);
      blocks.put(blockIndex, block);
    }
    return block;
  }
  
  public int size()
  {
    return size;
  }
  
  /**
   * 
   * this is the last call which represent the end of an object
   */
  public Slice toSlice()
  {
    return null;
  }
}
