package org.apache.apex.malhar.lib.window.accumulation;

import org.apache.commons.lang3.mutable.MutableLong;

public class MultiAccumulationMutableLong extends MultiAccumulation<Long, MutableLong>
{
  public static class AccumulationValuesMutableLong extends AbstractAccumulationValues<Long, MutableLong>
  {
    @Override
    protected void accumulateValue(AccumulationType type, Long value)
    {
      MutableLong oldValue = accumulationTypeToValue.get(type);
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
    protected void mergeValue(AccumulationType type, MutableLong otherValue)
    {
      accumulateValue(type, otherValue.longValue());
    }

    @Override
    protected double doubleValue(MutableLong value)
    {
      return value.longValue();
    }
  }

  public MultiAccumulationMutableLong(boolean includeCount, boolean includeAverage,
      AccumulationType... accumulationTypes)
  {
    defaultAccumulationValues = new AccumulationValuesMutableLong();
    setAccumulateTypes(includeCount, includeAverage, accumulationTypes);
  }

  public MultiAccumulationMutableLong()
  {
    this(true, true, AccumulationType.values());
  }
}
