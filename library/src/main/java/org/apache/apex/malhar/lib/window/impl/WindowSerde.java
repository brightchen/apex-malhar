package org.apache.apex.malhar.lib.window.impl;

import java.util.Map;

import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.netlet.util.Slice;

/**
 * 
 * This class can serialize/deserialize all window implementation.
 *
 */
public class WindowSerde implements Serde<Window, Slice>
{

  @Override
  public Slice serialize(Window object)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Window deserialize(Slice object, MutableInt offset)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Window deserialize(Slice object)
  {
    // TODO Auto-generated method stub
    return null;
  }

}
