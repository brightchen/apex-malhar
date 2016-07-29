package org.apache.apex.malhar.lib.state.spillable.managed;

import org.apache.apex.malhar.lib.state.managed.ManagedStateImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

@DefaultSerializer(FieldSerializer.class)
public class ManagedStateSpillableStateStore extends ManagedStateImpl implements SpillableStateStore
{
  public ManagedStateSpillableStateStore()
  {
    super();
  }
}
