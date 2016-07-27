package org.apache.apex.malhar.lib.window;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImplTest;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;

public class SpillableMapRecoveryTest extends SpillableByteArrayListMultimapImplTest
{
  public static class TestOperator extends BaseOperator
  {
    protected SpillableByteArrayListMultimapImpl<String, String> map;
    protected ManagedStateSpillableStateStore store;
    
    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      if(map == null)
        map = createMap(store);
      
      store.setup(context);
      map.setup(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void beginWindow(long windowId)
    {
      store.beginWindow(windowId);
      map.beginWindow(windowId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void endWindow()
    {
      map.endWindow();
      store.endWindow();
    }
  }
  
  @Test
  public void recoveryTest()
  {
    //recoveryTest(testMeta.store);
  }
  
  public void recoveryTest(SpillableStateStore store)
  {
    SpillableByteArrayListMultimapImpl<String, String> map = createMap(store);

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long nextWindowId = 1L;
    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);

    final String key = "window1";
    addData(store, map, key);
    
    map.endWindow();
    store.endWindow();
    
    store.beforeCheckpoint(nextWindowId);
    
    map.teardown();
    
    //create new map with same information
    map = createMap(store);
    Assert.assertNull(map.get(key));
    
    map.setup(testMeta.operatorContext);
    assertData(map, key);
    
    store.teardown();
  }
  
  public void addData(SpillableStateStore store, SpillableByteArrayListMultimapImpl<String, String> map, String key)
  {
    map.put(key, "a");

//    list1.addAll(Lists.newArrayList("a", "b", "c", "d", "e", "f", "g"));
    
    assertData(map, key);
  }
  
  public void assertData(SpillableByteArrayListMultimapImpl<String, String> map, String key)
  {
    List<String> list1 = map.get(key);
    
    Assert.assertEquals(1, list1.size());
    Assert.assertEquals("a", list1.get(0));
//    Assert.assertEquals("a", list1.get(1));
//    Assert.assertEquals("b", list1.get(2));
//    Assert.assertEquals("c", list1.get(3));
//    Assert.assertEquals("d", list1.get(4));
//    Assert.assertEquals("e", list1.get(5));
//    Assert.assertEquals("f", list1.get(6));
//    Assert.assertEquals("g", list1.get(7));
  }
  
  public static SpillableByteArrayListMultimapImpl<String, String> createMap(SpillableStateStore store)
  {
    return new SpillableByteArrayListMultimapImpl<String, String>(store, ID1, 0L, new SerdeStringSlice(),
        new SerdeStringSlice());
  }
}
