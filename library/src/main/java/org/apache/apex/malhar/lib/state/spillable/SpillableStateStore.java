package org.apache.apex.malhar.lib.state.spillable;

import org.apache.apex.malhar.lib.state.BucketedState;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;

public interface SpillableStateStore extends BucketedState, Component<Context.OperatorContext>,
    Operator.CheckpointNotificationListener
{
  public void beginWindow(long windowId);

  public void endWindow();
}
