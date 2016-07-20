package org.apache.apex.malhar.lib.utils.serde;

public interface SerToLVBuffer<T>
{
  public void serTo(T object, LVBuffer buffer);
}
