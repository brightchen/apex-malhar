package org.apache.apex.malhar.lib.window.accumulation;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.SumAccumulation;

public class CompositeAccumulationTest
{
  @Test
  public void testIncremental()
  {
    CompositeAccumulation<Long> accumulations = new CompositeAccumulation<>();
    int sumIndex = accumulations.addAccumulation((Accumulation)new SumAccumulation());
    int countIndex = accumulations.addAccumulation((Accumulation)new Count());
    int maxIndex = accumulations.addAccumulation(new Max());
    int minIndex = accumulations.addAccumulation(new Min());
    List values = accumulations.defaultAccumulatedValue();
    for (long i = 1; i <= 10; i++) {
      values = accumulations.accumulate(values, i);
    }

    List outputValues = accumulations.getOutput(values);
    Assert.assertTrue((Long)accumulations.getSubOutput(sumIndex, outputValues) == 55L);
    Assert.assertTrue((Long)accumulations.getSubOutput(countIndex, outputValues) == 10L);
    Assert.assertTrue((Long)accumulations.getSubOutput(maxIndex, outputValues) == 10L);
    Assert.assertTrue((Long)accumulations.getSubOutput(minIndex, outputValues) == 1L);
  }

  @Test
  public void testAverage()
  {
    CompositeAccumulation<Double> accumulations = new CompositeAccumulation<>();
    int averageIndex = accumulations.addAccumulation((Accumulation)new Average());
    List values = accumulations.defaultAccumulatedValue();
    for (int i = 1; i <= 10; i++) {
      values = accumulations.accumulate(values, i*1.0);
    }

    List outputValues = accumulations.getOutput(values);
    Assert.assertTrue((Double)accumulations.getSubOutput(averageIndex, outputValues) == 5.5);
  }
}
