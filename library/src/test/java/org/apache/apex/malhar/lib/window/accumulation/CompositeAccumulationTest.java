package org.apache.apex.malhar.lib.window.accumulation;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.SumAccumulation;
import org.apache.apex.malhar.lib.window.accumulation.CompositeAccumulation.AccumulationTag;

public class CompositeAccumulationTest
{
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testIncremental()
  {
    CompositeAccumulation<Long> accumulations = new CompositeAccumulation<>();
    AccumulationTag sumTag = accumulations.addAccumulation((Accumulation)new SumAccumulation());
    AccumulationTag countTag = accumulations.addAccumulation((Accumulation)new Count());
    AccumulationTag maxTag = accumulations.addAccumulation(new Max());
    AccumulationTag minTag = accumulations.addAccumulation(new Min());
    List values = accumulations.defaultAccumulatedValue();
    for (long i = 1; i <= 10; i++) {
      values = accumulations.accumulate(values, i);
    }

    List outputValues = accumulations.getOutput(values);
    Assert.assertTrue((Long)accumulations.getSubOutput(sumTag, outputValues) == 55L);
    Assert.assertTrue((Long)accumulations.getSubOutput(countTag, outputValues) == 10L);
    Assert.assertTrue((Long)accumulations.getSubOutput(maxTag, outputValues) == 10L);
    Assert.assertTrue((Long)accumulations.getSubOutput(minTag, outputValues) == 1L);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testAverage()
  {
    CompositeAccumulation<Double> accumulations = new CompositeAccumulation<>();
    AccumulationTag averageTag = accumulations.addAccumulation((Accumulation)new Average());
    List values = accumulations.defaultAccumulatedValue();
    for (int i = 1; i <= 10; i++) {
      values = accumulations.accumulate(values, i*1.0);
    }

    List outputValues = accumulations.getOutput(values);
    Assert.assertTrue((Double)accumulations.getSubOutput(averageTag, outputValues) == 5.5);
  }
}
