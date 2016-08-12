package com.datatorrent.benchmark.spillable;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class SpillableTestInputOperator extends BaseOperator implements InputOperator
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  public long count = 0;
  public int batchSize = 100;
  public int sleepBetweenBatch = 1;

  @Override
  public void emitTuples()
  {
    for (int i = 0; i < batchSize; ++i) {
      output.emit("" + ++count);
    }
    if (sleepBetweenBatch > 0) {
      try {
        Thread.sleep(sleepBetweenBatch);
      } catch (Exception e) {
        //ignore
      }
    }
  }
}