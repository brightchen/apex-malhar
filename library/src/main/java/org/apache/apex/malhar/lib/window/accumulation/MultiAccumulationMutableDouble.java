package org.apache.apex.malhar.lib.window.accumulation;

import org.apache.commons.lang3.mutable.MutableDouble;

public class MultiAccumulationMutableDouble extends MultiAccumulation<Double, MutableDouble, MultiAccumulationMutableDouble.AccumulationValuesMutableDouble>
{
  public static class AccumulationValuesMutableDouble extends MultiAccumulation.AbstractAccumulationValues<Double, MutableDouble>
  {
    @Override
    protected void accumulateValue(AccumulationType type, Double value)
    {
      MutableDouble oldValue = accumulationTypeToValue.get(type);
      if (oldValue == null) {
        accumulationTypeToValue.put(type, new MutableDouble(value));
        return;
      }
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
        default:
          throw new RuntimeException("Unexpected AccumulationType");
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
