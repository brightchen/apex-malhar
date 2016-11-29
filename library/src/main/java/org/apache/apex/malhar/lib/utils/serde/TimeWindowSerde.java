package org.apache.apex.malhar.lib.utils.serde;

import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.Window.TimeWindow;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TimeWindowSerde implements Serde<Window.TimeWindow>
{
  @Override
  public void serialize(TimeWindow timeWindow, Output output)
  {
    output.writeLong(timeWindow.getBeginTimestamp());
    output.writeLong(timeWindow.getDurationMillis());
  }

  @Override
  public TimeWindow deserialize(Input input)
  {
    return new TimeWindow(input.readLong(), input.readLong());
  }

}
