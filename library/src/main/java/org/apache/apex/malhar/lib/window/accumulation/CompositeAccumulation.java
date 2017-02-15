package org.apache.apex.malhar.lib.window.accumulation;

import java.util.List;

import org.apache.apex.malhar.lib.window.Accumulation;

import com.google.common.collect.Lists;

public class CompositeAccumulation<InputT> implements Accumulation<InputT, List, List>
{
  private List<Accumulation<InputT, Object, ?>> accumulations = Lists.newArrayList();

  /**
   * @param accumulation
   * @return The index of this accumulation. So the client can get the value of sub accumulation by index
   */
  public int addAccumulation(Accumulation<InputT, Object, ?> accumulation)
  {
    accumulations.add(accumulation);
    return accumulations.size() - 1;
  }

  public Object getSubOutput(int index, List output)
  {
    return accumulations.get(index).getOutput(output.get(index));
  }

  @Override
  public List defaultAccumulatedValue()
  {
    List defaultValues = Lists.newArrayList();
    for(Accumulation accumulation: accumulations) {
      defaultValues.add(accumulation.defaultAccumulatedValue());
    }
    return defaultValues;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public List accumulate(List accumulatedValues, InputT input)
  {
    for(int index = 0; index < accumulations.size(); ++index) {
      Accumulation accumulation = accumulations.get(index);
      Object oldValue = accumulatedValues.get(index);
      Object newValue = accumulation.accumulate(oldValue, input);
      if(newValue != oldValue) {
        accumulatedValues.set(index, newValue);
      }
    }
    return accumulatedValues;
  }

  @Override
  public List merge(List accumulatedValues1, List accumulatedValues2)
  {
    for(int index = 0; index < accumulations.size(); ++index) {
      accumulatedValues1.set(index, accumulations.get(index).merge(accumulatedValues1.get(index), accumulatedValues2.get(index)));
    }
    return accumulatedValues1;
  }

  @Override
  public List getOutput(List accumulatedValues)
  {
    return accumulatedValues;
  }

  @Override
  public List getRetraction(List values)
  {
    return values;
  }
}
