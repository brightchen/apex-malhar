package org.apache.apex.malhar.lib.utils.serde;

import com.datatorrent.netlet.util.Slice;

public interface SerToLVBuffer<T> extends Serde<T, Slice>
{
  void serTo(T object, LVBuffer buffer);
  
  void reset();
}
