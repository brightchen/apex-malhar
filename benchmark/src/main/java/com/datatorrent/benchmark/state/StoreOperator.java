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

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.ManagedTimeUnifiedStateImpl;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.benchmark.monitor.ResourceMonitorService;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.netlet.util.Slice;

public class StoreOperator extends BaseOperator implements Operator.CheckpointNotificationListener
{
  private static final Logger logger = LoggerFactory.getLogger(StoreOperator.class);

  public static enum ExecMode
  {
    INSERT,
    UPDATESYNC,
    UPDATEASYNC,
    GETSYNC,
    DONOTHING
  }

  protected static final int numOfWindowPerStatistics = 120;

  //this is the store we are going to use
  protected ManagedTimeUnifiedStateImpl store;

  protected long lastCheckPointWindowId = -1;
  protected long currentWindowId;
  protected long tupleCount = 0;
  protected int windowCountPerStatistics = 0;
  protected long statisticsBeginTime = 0;

  protected ExecMode execMode = ExecMode.INSERT;
  protected int timeRange = 1000 * 60;

  protected transient ResourceMonitorService monitorService;
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
    logger.info("The execute mode is: {}", execMode.name());
    store.setup(context);
    monitorService = ResourceMonitorService.create(60000).start();
  }

  @Override
  public void teardown()
  {
    monitorService.shutdownNow();
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

  /**
   * we verify 3 type of operation
   * @param tuple
   */
  protected Slice keySliceForRead = new Slice(null, 0, 0);
  protected void processTuple(KeyValPair<byte[], byte[]> tuple)
  {
    switch (execMode) {
      case UPDATEASYNC:
        //handle it specially
        updateAsync(tuple);
        break;


      case UPDATESYNC:
//        keySliceForRead.buffer = tuple.getKey();
//        keySliceForRead.offset = 0;
//        keySliceForRead.length = tuple.getKey().length;
//        store.getSync(getTimeByKey(tuple.getKey()), keySliceForRead);
        insertValueToStore(tuple);
        break;

      case GETSYNC:
        store.getSync(getTimeByKey(tuple.getKey()), new Slice(tuple.getKey()));
        break;

      case DONOTHING:
        break;

      default: //insert
        insertValueToStore(tuple);
    }

    ++tupleCount;
  }

  protected transient long sameKeyCount = 0;
  protected transient long preKey = -1;
  protected long getTimeByKey(byte[] key)
  {
    long lKey = ByteBuffer.wrap(key).getLong();
    lKey = lKey - (lKey % timeRange);
    if (preKey == lKey) {
      sameKeyCount++;
    } else {
      logger.info("key: {} count: {}", preKey, sameKeyCount);
      preKey = lKey;
      sameKeyCount = 1;
    }
    return lKey;
  }

  // give a barrier to avoid used up memory
  protected final int taskBarrier = 100000;

  /**
   * This method first send request of get to the state manager, then handle all the task(get) which already done and update the value.
   * @param tuple
   */
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

    //send request of get to the state manager and add to the taskQueue and taskToPair.
    //the reason of an extra taskQueue to make sure the tasks are ordered
    {
      Slice key = new Slice(tuple.getKey());
      Future<Slice> task = store.getAsync(getTimeByKey(tuple.getKey()), key);
      taskQueue.add(task);
      taskToPair.put(task, tuple);
    }

    //handle all the tasks which have finished
    while (!taskQueue.isEmpty()) {
      //assume task finished in order.
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

    store.put(System.currentTimeMillis(), key, value);
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

  public ManagedTimeUnifiedStateImpl getStore()
  {
    return store;
  }

  public void setStore(ManagedTimeUnifiedStateImpl store)
  {
    this.store = store;
  }

  protected void logStatistics()
  {
    long spentTime = System.currentTimeMillis() - statisticsBeginTime;
    logger.info("Windows: {}; Time Spent: {}, Processed tuples: {}, rate per second: {}", windowCountPerStatistics, spentTime, tupleCount, tupleCount * 1000 / spentTime);

    statisticsBeginTime = System.currentTimeMillis();
    tupleCount = 0;
  }

  public ExecMode getExecMode()
  {
    return execMode;
  }

  public void setExecMode(ExecMode execMode)
  {
    this.execMode = execMode;
  }

  public String getExecModeString()
  {
    return execMode.name();
  }

  public void setExecModeStr(String execModeStr)
  {
    //this method used for set configuration. so, use case-insensitive
    for (ExecMode em : ExecMode.values()) {
      if (em.name().equalsIgnoreCase(execModeStr)) {
        this.execMode = em;
      }
    }
  }

  public int getTimeRange()
  {
    return timeRange;
  }

  public void setTimeRange(int timeRange)
  {
    this.timeRange = timeRange;
  }

}
