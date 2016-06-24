package org.apache.apex.malhar.lib.state.spillable.inmem;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.util.TestUtils;

/**
 * Created by tfarkas on 6/6/16.
 */
public class InMemorySpillableStateStoreTest
{
  @Test
  public void simpleStoreTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    store.setup(null);

    long windowId = 0L;
    store.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(null, store.getSync(0L, TestUtils.getSlice(1)));

    store.put(0L, TestUtils.getSlice(1), TestUtils.getSlice(2));
    store.put(0L, TestUtils.getSlice(3), TestUtils.getSlice(10));
    store.put(1L, TestUtils.getSlice(2), TestUtils.getSlice(3));
    store.put(1L, TestUtils.getSlice(4), TestUtils.getSlice(11));

    Assert.assertEquals(null, store.getSync(0L, TestUtils.getSlice(2)));
    Assert.assertEquals(null, store.getSync(0L, TestUtils.getSlice(4)));
    Assert.assertEquals(TestUtils.getSlice(2), store.getSync(0L, TestUtils.getSlice(1)));
    Assert.assertEquals(TestUtils.getSlice(10), store.getSync(0L, TestUtils.getSlice(3)));

    Assert.assertEquals(null, store.getSync(1L, TestUtils.getSlice(1)));
    Assert.assertEquals(null, store.getSync(1L, TestUtils.getSlice(3)));
    Assert.assertEquals(TestUtils.getSlice(3), store.getSync(1L, TestUtils.getSlice(2)));
    Assert.assertEquals(TestUtils.getSlice(11), store.getSync(1L, TestUtils.getSlice(4)));

    store.endWindow();

    store.teardown();
  }
}
