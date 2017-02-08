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
import java.util.Set;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.commons.lang3.mutable.MutableLong;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class MultiAccumulation<InputT extends Number, AccumT, AV extends MultiAccumulation.AccumulationValues>
    implements Accumulation<InputT, AV, AV>
{
  public static enum AccumulationType
  {
    MAX,
    MIN,
    SUM
  }

  public static interface AccumulationValues<InputT, AccumT>
  {
    public void setAccumulateTypes(boolean includeCount, boolean includeAverage, AccumulationType ... accumulationTypes);

    public void accumulateValue(InputT value);

    public void merge(AccumulationValues<InputT, AccumT> otherValue);

    public AccumT getAccumulation(AccumulationType zccumulationType);

    public MutableLong getCount();

    public double getAverage();
  }

  public abstract static class AbstractAccumulationValues<InputT, AccumT> implements AccumulationValues<InputT, AccumT>
  {
    protected Map<AccumulationType, AccumT> accumulationTypeToValue = Maps.newEnumMap(AccumulationType.class);
    protected MutableLong count = new MutableLong(0);

    protected boolean includeCount;
    protected Set<AccumulationType> accumulationTypes;

    @Override
    public void setAccumulateTypes(boolean includeCount, boolean includeAverage, AccumulationType ... accumulationTypes)
    {
      this.includeCount = includeCount;
      this.accumulationTypes = accumulationTypes == null ? null : Sets.newHashSet(accumulationTypes);
      if (includeAverage) {
        if (this.accumulationTypes == null) {
          this.accumulationTypes = Sets.newHashSet(AccumulationType.SUM);
        } else {
          this.accumulationTypes.add(AccumulationType.SUM);
        }
        this.includeCount = true;
      }
    }

    @Override
    public AccumT getAccumulation(AccumulationType accumulationType)
    {
      return accumulationTypeToValue.get(accumulationType);
    }

    @Override
    public MutableLong getCount()
    {
      return count;
    }

    @Override
    public double getAverage()
    {
      return doubleValue(accumulationTypeToValue.get(AccumulationType.SUM)) / getCount().longValue();
    }

    protected abstract double doubleValue(AccumT value);

    @Override
    public void accumulateValue(InputT value)
    {
      for (AccumulationType type : accumulationTypes) {
        accumulateValue(type, value);
      }
    }

    protected abstract void accumulateValue(AccumulationType type, InputT value);

    @Override
    public void merge(AccumulationValues<InputT, AccumT> otherValues)
    {
      for (AccumulationType type : accumulationTypes) {
        mergeValue(type, otherValues.getAccumulation(type));
      }
    }

    protected abstract void mergeValue(AccumulationType type, AccumT otherValue);
  }

//  @Override
//  public AccumulationValues<InputT, AccumT> defaultAccumulatedValue()
//  {
//    return new AbstractAccumulationValues();
//  }


  protected AV defaultAccumulationValues;

  public void setAccumulateTypes(boolean includeCount, boolean includeAverage, AccumulationType ... accumulationTypes)
  {
    defaultAccumulationValues.setAccumulateTypes(includeCount, includeAverage, accumulationTypes);
  }

  @Override
  public AV defaultAccumulatedValue()
  {
    return defaultAccumulationValues;
  }

  @Override
  public AV accumulate(AV accumulatedValue, InputT input)
  {
    accumulatedValue.getCount().increment();
    accumulatedValue.accumulateValue(input);
    return accumulatedValue;
  }


  @Override
  public AV merge(AV accumulatedValue1, AV accumulatedValue2)
  {
    accumulatedValue1.getCount().add(accumulatedValue2.getCount());
    accumulatedValue1.merge(accumulatedValue2);
    return accumulatedValue1;
  }

  @Override
  public AV getOutput(AV accumulatedValue)
  {
    return accumulatedValue;
  }

  @Override
  public AV getRetraction(AccumulationValues value)
  {
    return null;
  }

}
