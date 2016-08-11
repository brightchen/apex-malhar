package org.apache.apex.malhar.lib.utils.serde;

import com.datatorrent.netlet.util.Slice;

public interface BufferStream
{
  void write(byte[] data);
  void write(byte[] data, final int offset, final int length);
  int size();
  Slice toSlice();
  void reset();
}
