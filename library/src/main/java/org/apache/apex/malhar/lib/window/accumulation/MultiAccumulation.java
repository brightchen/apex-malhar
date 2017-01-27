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

import java.util.Map;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.MutablePair;

import com.google.common.collect.Maps;
public abstract class MultiAccumulation<InputT extends Number> implements Accumulation<InputT, MutablePair<MutableLong, Map<MultiAccumulation.AccumulationType, InputT>>, MutablePair<MutableLong, Map<MultiAccumulation.AccumulationType, InputT>>>
{
  public static enum AccumulationType
  {
    MAX,
    MIN,
    SUM
  }

  @Override
  public MutablePair<MutableLong, Map<AccumulationType, InputT>> defaultAccumulatedValue()
  {
    Map<MultiAccumulation.AccumulationType, InputT> values = Maps.newEnumMap(AccumulationType.class);

    return new MutablePair<>(new MutableLong(0), values);
  }

  @Override
  public MutablePair<MutableLong, Map<AccumulationType, InputT>> accumulate(
      MutablePair<MutableLong, Map<AccumulationType, InputT>> accumulatedValue, InputT input)
  {
    accumulatedValue.left.add(1);

    Map<AccumulationType, InputT> values = accumulatedValue.right;

    {
      InputT oldValue = values.get(AccumulationType.MAX);
      InputT newValue = oldValue == null ? null : max(oldValue, input);
      values.put(AccumulationType.MAX, newValue);
    }

    {
      InputT oldValue = values.get(AccumulationType.MIN);
      InputT newValue = oldValue == null ? null : min(oldValue, input);
      values.put(AccumulationType.MIN, newValue);
    }

    {
      InputT oldValue = values.get(AccumulationType.SUM);
      InputT newValue = oldValue == null ? null : min(oldValue, input);
      values.put(AccumulationType.SUM, newValue);
    }

    return accumulatedValue;
  }

  protected abstract InputT min(InputT oldValue, InputT input);

  protected abstract InputT max(InputT oldValue, InputT input);

  protected abstract InputT sum(InputT oldValue, InputT input);

  @Override
  public MutablePair<MutableLong, Map<AccumulationType, InputT>> merge(
      MutablePair<MutableLong, Map<AccumulationType, InputT>> accumulatedValue1,
      MutablePair<MutableLong, Map<AccumulationType, InputT>> accumulatedValue2)
  {
    accumulatedValue1.getLeft().add(accumulatedValue2.getLeft().longValue());

    Map<AccumulationType, InputT> values1 = accumulatedValue1.right;
    Map<AccumulationType, InputT> values2 = accumulatedValue2.right;

    {
      InputT oldValue = values1.get(AccumulationType.MAX);
      InputT newValue = oldValue == null ? null : max(oldValue, values2.get(AccumulationType.MAX));
      values1.put(AccumulationType.MAX, newValue);
    }

    {
      InputT oldValue = values1.get(AccumulationType.MIN);
      InputT newValue = oldValue == null ? null : min(oldValue, values2.get(AccumulationType.MIN));
      values1.put(AccumulationType.MIN, newValue);
    }

    {
      InputT oldValue = values1.get(AccumulationType.SUM);
      InputT newValue = oldValue == null ? null : sum(oldValue, values2.get(AccumulationType.SUM));
      values1.put(AccumulationType.SUM, newValue);
    }

    return accumulatedValue1;
  }

  @Override
  public MutablePair<MutableLong, Map<AccumulationType, InputT>> getOutput(
      MutablePair<MutableLong, Map<AccumulationType, InputT>> accumulatedValue)
  {
    return accumulatedValue;
  }

  @Override
  public MutablePair<MutableLong, Map<AccumulationType, InputT>> getRetraction(
      MutablePair<MutableLong, Map<AccumulationType, InputT>> value)
  {
    // TODO
    return null;
  }
}
