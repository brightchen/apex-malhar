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
package com.datatorrent.benchmark.window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponentImpl;
import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.apex.malhar.lib.window.accumulation.Count;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.lib.window.impl.SpillableWindowedKeyedStorage;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyValPair;

public class KeyedWindowedOperatorBenchmarkApp extends AbstractWindowedOperatorBenchmarkApp<KeyedWindowedOperatorBenchmarkApp.KeyedWindowedGenerator, KeyedWindowedOperatorBenchmarkApp.MyKeyedWindowedOperator>
{
  public KeyedWindowedOperatorBenchmarkApp()
  {
    generatorClass = KeyedWindowedGenerator.class;
    windowedOperatorClass = KeyedWindowedOperatorBenchmarkApp.MyKeyedWindowedOperator.class;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void connectGeneratorToWindowedOperator(DAG dag, KeyedWindowedGenerator generator,
      KeyedWindowedOperatorBenchmarkApp.MyKeyedWindowedOperator windowedOperator)
  {
    dag.addStream("Data", generator.data, windowedOperator.input).setLocality(Locality.CONTAINER_LOCAL);
  }

  @Override
  protected void setOtherStorage(MyKeyedWindowedOperator windowedOperator, SpillableComplexComponentImpl sccImpl)
  {
    windowedOperator.setUpdatedDataStorage(createUpdatedDataStorage(sccImpl));
  }


  protected static class MyKeyedWindowedOperator extends KeyedWindowedOperatorImpl
  {
    private static final Logger logger = LoggerFactory.getLogger(MyKeyedWindowedOperator.class);

    private long logWindows = 20;
    private long windowCount = 0;
    private long beginTime = System.currentTimeMillis();
    private long tupleCount = 0;
    private long totalBeginTime = System.currentTimeMillis();
    private long totalCount = 0;

    private long droppedCount = 0;
    @Override
    public void dropTuple(Tuple input)
    {
      droppedCount++;
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
      if (++windowCount == logWindows) {
        long endTime = System.currentTimeMillis();
        tupleCount -= droppedCount;
        totalCount += tupleCount;
        logger.info("total: count: {}; time: {}; average: {}; period: count: {}; dropped: {}; time: {}; average: {}",
            totalCount, endTime - totalBeginTime, totalCount * 1000 / (endTime - totalBeginTime),
            tupleCount, droppedCount, endTime - beginTime, tupleCount * 1000 / (endTime - beginTime));
        windowCount = 0;
        beginTime = System.currentTimeMillis();
        tupleCount = 0;
        droppedCount = 0;
      }
    }

    @Override
    public void processTuple(Tuple tuple)
    {
      super.processTuple(tuple);
      ++tupleCount;
    }
  }

  protected static class KeyedWindowedGenerator extends AbstractGenerator
  {
    public final transient DefaultOutputPort<Tuple.TimestampedTuple<KeyValPair<String, Long>>> data = new DefaultOutputPort<Tuple.TimestampedTuple<KeyValPair<String, Long>>>();

    @Override
    public void emitTuples()
    {
      for (int i = 0; i < emitBatchSize && emitCount < rate; i++) {
        data.emit(new Tuple.TimestampedTuple<KeyValPair<String, Long>>(System.currentTimeMillis() - random.nextInt(60000),
            new KeyValPair<String, Long>("" + random.nextInt(100000), (long)random.nextInt(100))));
        emitCount++;
      }
    }
  }

  @Override
  protected Accumulation createAccumulation()
  {
    return new Count();
  }

  private boolean useInMemoryStorage = false;
  @Override
  protected WindowedStorage createDataStorage(SpillableComplexComponentImpl sccImpl)
  {
    if (useInMemoryStorage) {
      return new InMemoryWindowedKeyedStorage();
    }
    SpillableWindowedKeyedStorage dataStorage = new SpillableWindowedKeyedStorage();
    dataStorage.setSpillableComplexComponent(sccImpl);
    return dataStorage;
  }

  protected WindowedStorage.WindowedKeyedStorage createUpdatedDataStorage(SpillableComplexComponentImpl sccImpl)
  {
    if (useInMemoryStorage) {
      return new InMemoryWindowedKeyedStorage();
    }
    SpillableWindowedKeyedStorage dataStorage = new SpillableWindowedKeyedStorage();
    dataStorage.setSpillableComplexComponent(sccImpl);
    return dataStorage;
  }

  @Override
  protected WindowedStorage createRetractionStorage(SpillableComplexComponentImpl sccImpl)
  {
    if (useInMemoryStorage) {
      return new InMemoryWindowedKeyedStorage();
    }
    SpillableWindowedKeyedStorage retractionStorage = new SpillableWindowedKeyedStorage();
    retractionStorage.setSpillableComplexComponent(sccImpl);
    return retractionStorage;
  }

}
