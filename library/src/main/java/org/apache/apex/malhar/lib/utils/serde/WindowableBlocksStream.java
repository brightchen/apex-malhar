/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.utils.serde;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class WindowableBlocksStream extends BlocksStream implements WindowableByteStream
{
  private static final Logger logger = LoggerFactory.getLogger(WindowableBlocksStream.class);
  /**
   * Map from windowId to blockIds
   */
  protected SetMultimap<Long, Integer> windowToBlockIds = HashMultimap.create();

  /**
   * set of all free blockIds.
   */
  protected Set<Integer> freeBlockIds = Sets.newHashSet();

  // max block index; valid maxBlockIndex should >= 0
  protected int maxBlockIndex = 0;

  protected long currentWindowId;

  public WindowableBlocksStream()
  {
    super();
  }

  public WindowableBlocksStream(int blockCapacity)
  {
    super(blockCapacity);
  }

  /**
   * 
   * windowToBlockIds.put(currentWindowId, currentBlockIndex);
   */
  @Override
  public void write(byte[] data, final int offset, final int length)
  {
    super.write(data, offset, length);
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    moveToNextWindow();
  }

  /**
   * make sure different windows will not use same block. Move to next block if
   * current block already used.
   */
  protected void moveToNextWindow()
  {
    //use current block if it hasn't be used, else, move to next block
    FixedBlock block = getOrCreateCurrentBlock();
    if (!block.isClear()) {
      throw new RuntimeException("Current block not clear, should NOT move to next window. Please call toSlice() to output data first");
    }
    if (block.size() > 0) {
      moveToNextBlock();
    }
  }

  /**
   * This method try to use the free block first. Allocate new block if there
   * are no any free block
   * 
   * @return The previous block
   */
  @Override
  protected FixedBlock moveToNextBlock()
  {
    FixedBlock previousBlock = currentBlock;
    if (!freeBlockIds.isEmpty()) {
      currentBlockIndex = freeBlockIds.iterator().next();
      freeBlockIds.remove(currentBlockIndex);
      currentBlock = this.blocks.get(currentBlockIndex);
    } else {
      currentBlockIndex = ++maxBlockIndex;
      currentBlock = getOrCreateCurrentBlock();
    }

    windowToBlockIds.put(currentWindowId, currentBlockIndex);
    
    return previousBlock;
  }

  /**
   * probably need to call this method. call beginWindow(long) should be enough
   */
  @Override
  public void endWindow()
  {
  }

  @Override
  public void resetUpToWindow(long windowId)
  {
    Set<Long> winIds = Sets.newHashSet(windowToBlockIds.keySet());
    int removedSize = 0;
    for (long winId : winIds) {
      if (winId <= windowId) {
        Set<Integer> removedBlockIds = windowToBlockIds.removeAll(winId);

        for (int blockId : removedBlockIds) {
          removedSize += blocks.get(blockId).size();
          Block theBlock = blocks.get(blockId);
          theBlock.reset();
          if (theBlock == currentBlock) {
            //the client code could ask reset up to current window
            //but the reset block should not be current block. current block should be reassigned.
            moveToNextBlock();
          }
          logger.debug("reset block: {}, currentBlock: {}", blockId, theBlock);
        }
        
        freeBlockIds.addAll(removedBlockIds);
      }
    }
    
    size -= removedSize;
  }

  @Override
  public void reset()
  {
    super.reset();
    
    //all blocks are free now except the current one
    freeBlockIds.addAll(blocks.keySet());
    freeBlockIds.remove(currentBlockIndex);

  }
}
