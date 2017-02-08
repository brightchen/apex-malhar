package org.apache.apex.malhar.lib.window.accumulation;

import org.apache.commons.lang3.mutable.MutableDouble;

public class MultiAccumulationMutableDouble extends MultiAccumulation<Double, MutableDouble>
{
  public static class AccumulationValuesMutableDouble extends AbstractAccumulationValues<Double, MutableDouble>
  {
    @Override
    protected void accumulateValue(AccumulationType type, Double value)
    {
      MutableDouble oldValue = accumulationTypeToValue.get(type);
      switch (type) {
        case MAX:
          if (oldValue.longValue() < value) {
            oldValue.setValue(value);
          }
          break;
        case MIN:
          if (oldValue.longValue() > value) {
            oldValue.setValue(value);
          }
          break;
        case SUM:
          oldValue.add(value);
          break;
      }
    }

    @Override
    protected void mergeValue(AccumulationType type, MutableDouble otherValue)
    {
      accumulateValue(type, otherValue.doubleValue());
    }

    @Override
    protected double doubleValue(MutableDouble value)
    {
      return value.doubleValue();
    }
  }

  public MultiAccumulationMutableDouble(boolean includeCount, boolean includeAverage, AccumulationType ... accumulationTypes)
  {
    defaultAccumulationValues = new AccumulationValuesMutableDouble();
    setAccumulateTypes(includeCount, includeAverage, accumulationTypes);
  }

  public MultiAccumulationMutableDouble()
  {
    this(true, true, AccumulationType.values());
  }
}
