package org.apache.apex.malhar.lib.state.spillable;

import java.util.List;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;

/**
 * Created by tfarkas on 6/12/16.
 */
public abstract class AbstractSpillableComplexComponent implements SpillableComplexComponent
{
  private List<SpillableComponent> componentList = Lists.newArrayList();

  protected void addSpillableComponent(Spillable.SpillableComponent spillableComponent)
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.setup(context);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.beginWindow(windowId);
    }
  }

  @Override
  public void endWindow()
  {
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.endWindow();
    }
  }

  @Override
  public void teardown()
  {
    for (SpillableComponent spillableComponent: componentList) {
      spillableComponent.teardown();
    }
  }
}
