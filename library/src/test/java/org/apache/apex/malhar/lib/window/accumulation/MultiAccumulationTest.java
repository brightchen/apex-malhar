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
package org.apache.apex.malhar.lib.window.accumulation;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.accumulation.MultiAccumulation.AccumulationType;

public class MultiAccumulationTest
{
  @Test
  public void testLong()
  {
    MultiAccumulationLong accumulation = new MultiAccumulationLong();
    MultiAccumulationLong.AccumulationValuesLong value = accumulation.defaultAccumulatedValue();
    for (long i = 1; i <= 10; i++) {
      value = accumulation.accumulate(value, i);
    }
    Assert.assertTrue(value.getCount().longValue() == 10L);
    Assert.assertTrue(value.getAccumulation(AccumulationType.MAX) == 10L);
    Assert.assertTrue(value.getAccumulation(AccumulationType.MIN) == 1L);
    Assert.assertTrue(value.getAccumulation(AccumulationType.SUM) == 55L);
    Assert.assertTrue(value.getAverage() == 5.5);
  }

  @Test
  public void testMutableLong()
  {
    MultiAccumulationMutableLong accumulation = new MultiAccumulationMutableLong();
    MultiAccumulationMutableLong.AccumulationValuesMutableLong value = accumulation.defaultAccumulatedValue();
    for (long i = 1; i <= 10; i++) {
      value = accumulation.accumulate(value, i);
    }
    Assert.assertTrue(value.getCount().longValue() == 10L);
    Assert.assertTrue(value.getAccumulation(AccumulationType.MAX).longValue() == 10L);
    Assert.assertTrue(value.getAccumulation(AccumulationType.MIN).longValue() == 1L);
    Assert.assertTrue(value.getAccumulation(AccumulationType.SUM).longValue() == 55L);
    Assert.assertTrue(value.getAverage() == 5.5);
  }

  @Test
  public void testDouble()
  {
    MultiAccumulationDouble accumulation = new MultiAccumulationDouble();
    MultiAccumulationDouble.AccumulationValuesDouble value = accumulation.defaultAccumulatedValue();
    for (long i = 1; i <= 10; i++) {
      value = accumulation.accumulate(value, i * 1.0);
    }
    Assert.assertTrue(value.getCount().longValue() == 10L);
    Assert.assertTrue(value.getAccumulation(AccumulationType.MAX) == 10.0);
    Assert.assertTrue(value.getAccumulation(AccumulationType.MIN) == 1.0);
    Assert.assertTrue(value.getAccumulation(AccumulationType.SUM) == 55.0);
    Assert.assertTrue(value.getAverage() == 5.5);
  }

  @Test
  public void testMutableDouble()
  {
    MultiAccumulationMutableDouble accumulation = new MultiAccumulationMutableDouble();
    MultiAccumulationMutableDouble.AccumulationValuesMutableDouble value = accumulation.defaultAccumulatedValue();
    for (long i = 1; i <= 10; i++) {
      value = accumulation.accumulate(value, i * 1.0);
    }
    Assert.assertTrue(value.getCount().longValue() == 10L);
    Assert.assertTrue(value.getAccumulation(AccumulationType.MAX).doubleValue() == 10.0);
    Assert.assertTrue(value.getAccumulation(AccumulationType.MIN).doubleValue() == 1.0);
    Assert.assertTrue(value.getAccumulation(AccumulationType.SUM).doubleValue() == 55.0);
    Assert.assertTrue(value.getAverage() == 5.5);
  }

  @Test
  public void testLongConfig()
  {
    MultiAccumulationLong accumulation = new MultiAccumulationLong(true, true, null);
    MultiAccumulationLong.AccumulationValuesLong value = accumulation.defaultAccumulatedValue();
    for (long i = 1; i <= 10; i++) {
      value = accumulation.accumulate(value, i);
    }
    Assert.assertTrue(value.getCount().longValue() == 10L);
    Assert.assertTrue(value.getAccumulation(AccumulationType.SUM) == 55L);
    Assert.assertTrue(value.getAverage() == 5.5);

    Assert.assertTrue(value.getAccumulation(AccumulationType.MAX) == null);
    Assert.assertTrue(value.getAccumulation(AccumulationType.MIN) == null);
  }
}
