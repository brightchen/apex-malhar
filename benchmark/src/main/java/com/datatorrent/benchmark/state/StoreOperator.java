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
package com.datatorrent.benchmark.state;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.ManagedStateImpl;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.netlet.util.Slice;

public class StoreOperator extends BaseOperator implements Operator.CheckpointNotificationListener
{
  private static final Logger logger = LoggerFactory.getLogger(StoreOperator.class);

  public static enum ExeMode
  {
    Insert,
    UpdateSync,
    UpdateAsync
  }
  
  protected static final int numOfWindowPerStatistics = 10;

  protected ManagedStateImpl store;
  protected long bucketId = 1;

  protected long lastCheckPointWindowId = -1;
  protected long currentWindowId;
  protected long tupleCount = 0;
  protected int windowCountPerStatistics = 0;
  protected long statisticsBeginTime = 0;
  
  protected ExeMode exeMode = ExeMode.Insert;

  public final transient DefaultInputPort<KeyValPair<byte[], byte[]>> input = new DefaultInputPort<KeyValPair<byte[], byte[]>>()
  {
    @Override
    public void process(KeyValPair<byte[], byte[]> tuple)
    {
      processTuple(tuple);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    logger.info("The execute mode is: {}", exeMode.name());
    store.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    store.beginWindow(windowId);
    if (statisticsBeginTime <= 0) {
      statisticsBeginTime = System.currentTimeMillis();
    }
  }

  @Override
  public void endWindow()
  {
    store.endWindow();
    if (++windowCountPerStatistics >= numOfWindowPerStatistics) {
      logStatistics();
      windowCountPerStatistics = 0;
    }
  }

  protected transient Queue<Future<Slice>> taskQueue = new LinkedList<Future<Slice>>();
  protected transient Map<Future<Slice>, KeyValPair<byte[], byte[]>> taskToPair = Maps.newHashMap();
  
  protected void processTuple(KeyValPair<byte[], byte[]> tuple)
  {
    if (ExeMode.UpdateAsync == exeMode) {
      updateAsync(tuple);
      return;
    }

    Slice key = new Slice(tuple.getKey());

    if (ExeMode.UpdateSync == exeMode) {
      store.getSync(bucketId, key);
    }
    insertValueToStore(tuple);
  }
  
  protected final int taskBarrier = 1000000;
  protected void updateAsync(KeyValPair<byte[], byte[]> tuple)
  {
    if (taskQueue.size() > taskBarrier) {
      //slow down to avoid too much task waiting.
      try {
        
        logger.info("Queue Size: {}, wait time(milli-seconds): {}", taskQueue.size(), taskQueue.size() / taskBarrier);
        Thread.sleep(taskQueue.size() / taskBarrier);
      } catch (Exception e) {
        //ignore
      }
    }
    
    {
      Slice key = new Slice(tuple.getKey());
      Future<Slice> task = store.getAsync(bucketId, key);
      taskQueue.add(task);
      taskToPair.put(task, tuple);
    }

    while (!taskQueue.isEmpty()) {
      //assume task finished in sequence.
      if (!taskQueue.peek().isDone()) {
        break;
      }

      Future<Slice> task = taskQueue.poll();
      insertValueToStore(taskToPair.remove(task));
    }
  }
  
  protected void insertValueToStore(KeyValPair<byte[], byte[]> tuple)
  {
    Slice key = new Slice(tuple.getKey());
    Slice value = new Slice(tuple.getValue());

    store.put(bucketId, key, value);
    ++tupleCount;
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    store.committed(windowId);
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    store.beforeCheckpoint(windowId);
    logger.info("beforeCheckpoint {}", windowId);
  }

  public ManagedStateImpl getStore()
  {
    return store;
  }

  public void setStore(ManagedStateImpl store)
  {
    this.store = store;
  }

  protected void logStatistics()
  {
    long spentTime = System.currentTimeMillis() - statisticsBeginTime;
    logger.info("Time Spent: {}, Processed tuples: {}, rate per second: {}", spentTime, tupleCount, tupleCount * 1000 / spentTime);

    statisticsBeginTime = System.currentTimeMillis();
    tupleCount = 0;
  }

  public ExeMode getExeMode()
  {
    return exeMode;
  }

  public void setExeMode(ExeMode exeMode)
  {
    this.exeMode = exeMode;
  }

  public String getExeModeString()
  {
    return exeMode.name();
  }
  
  public void setExeModeStr(String exeModeStr)
  {
    this.exeMode = ExeMode.valueOf(exeModeStr);
  }
  
}
