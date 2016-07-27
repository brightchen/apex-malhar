package org.apache.apex.malhar.lib.window;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImplTest;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeIntSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;

import com.google.common.collect.Lists;

import com.datatorrent.netlet.util.Slice;

public class SpillableMapRecoveryTest extends SpillableByteArrayListMultimapImplTest
{
  @Test
  public void recoveryTest()
  {
    recoveryTest(testMeta.store);
  }
  
  public void recoveryTest(SpillableStateStore store)
  {
    SpillableByteArrayListMultimapImpl<String, String> map = createMap(store);

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long nextWindowId = 0L;
    nextWindowId = simpleMultiKeyTestHelper(store, map, "a", nextWindowId);
    nextWindowId = simpleMultiKeyTestHelper(store, map, "b", nextWindowId);
    simpleMultiKeyTestHelper(store, map, "c", nextWindowId);

    map.teardown();
    
    //create new map with same information
    map = createMap(store);
    
    store.teardown();
  }
  
  public void addData(SpillableStateStore store, SpillableByteArrayListMultimapImpl<String, String> map, String key, long nextWindowId)
  {
    SerdeStringSlice serdeString = new SerdeStringSlice();
    SerdeIntSlice serdeInt = new SerdeIntSlice();

    Slice keySlice = serdeString.serialize(key);

    byte[] keyBytes = SliceUtils.concatenate(ID1, keySlice.toByteArray());

    store.beginWindow(nextWindowId);
    map.beginWindow(nextWindowId);
    nextWindowId++;

    Assert.assertNull(map.get(key));

    Assert.assertFalse(map.containsKey(key));

    map.put(key, "a");

    Assert.assertTrue(map.containsKey(key));

    List<String> list1 = map.get(key);
    Assert.assertEquals(1, list1.size());

    Assert.assertEquals("a", list1.get(0));

    list1.addAll(Lists.newArrayList("a", "b", "c", "d", "e", "f", "g"));
  }
  public SpillableByteArrayListMultimapImpl createMap(SpillableStateStore store)
  {
    return new SpillableByteArrayListMultimapImpl<String, String>(store, ID1, 0L, new SerdeStringSlice(),
        new SerdeStringSlice());
  }
}
