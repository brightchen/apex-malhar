package org.apache.apex.malhar.lib.state.spillable;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.state.managed.ManagedStateTestUtils;
import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;

import com.datatorrent.api.Context;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.util.TestUtils;

/**
 * Created by tfarkas on 6/6/16.
 */
public class SpillableByteMapImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};
  public static final byte[] ID2 = new byte[]{(byte)1};

  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();

  @Test
  public void simpleGetAndPutTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    simpleGetAndPutTestHelper(store);
  }

  @Test
  public void simpleGetAndPutManagedStateTest()
  {
    simpleGetAndPutTestHelper(testMeta.store);
  }

  private void simpleGetAndPutTestHelper(SpillableStateStore store)
  {
    SerdeStringSlice sss = new SerdeStringSlice();

    SpillableByteMapImpl<String, String> map = new SpillableByteMapImpl<>(store, ID1, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice());

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    map.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(0, map.size());

    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");

    Assert.assertEquals(3, map.size());

    Assert.assertEquals("1", map.get("a"));
    Assert.assertEquals("2", map.get("b"));
    Assert.assertEquals("3", map.get("c"));
    Assert.assertEquals(null, map.get("d"));

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    store.beginWindow(windowId);
    map.beginWindow(windowId);
    windowId++;

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, "2");
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, "3");
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);

    Assert.assertEquals(3, map.size());

    Assert.assertEquals("1", map.get("a"));
    Assert.assertEquals("2", map.get("b"));
    Assert.assertEquals("3", map.get("c"));
    Assert.assertEquals(null, map.get("d"));

    map.put("d", "4");
    map.put("e", "5");
    map.put("f", "6");

    Assert.assertEquals(6, map.size());

    Assert.assertEquals("4", map.get("d"));
    Assert.assertEquals("5", map.get("e"));
    Assert.assertEquals("6", map.get("f"));

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, "2");
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, "3");
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, null);

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    store.beginWindow(windowId);
    map.beginWindow(windowId);
    windowId++;

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, "2");
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, "3");
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, "4");
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, "5");
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, "6");
    SpillableTestUtils.checkValue(store, 0L, "g", ID1, null);

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    map.teardown();
    store.teardown();
  }

  @Test
  public void simpleRemoveTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    simpleRemoveTestHelper(store);
  }

  @Test
  public void simpleRemoveManagedStateTest()
  {
    simpleRemoveTestHelper(testMeta.store);
  }

  private void simpleRemoveTestHelper(SpillableStateStore store)
  {
    SerdeStringSlice sss = new SerdeStringSlice();

    SpillableByteMapImpl<String, String> map = new SpillableByteMapImpl<>(store, ID1, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice());

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    map.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(0, map.size());

    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");

    Assert.assertEquals(3, map.size());

    map.remove("b");
    map.remove("c");

    Assert.assertEquals("1", map.get("a"));
    Assert.assertEquals(null, map.get("b"));
    Assert.assertEquals(null, map.get("c"));
    Assert.assertEquals(null, map.get("d"));

    Assert.assertEquals(1, map.size());

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);

    store.beginWindow(windowId);
    map.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(1, map.size());

    Assert.assertEquals("1", map.get("a"));
    Assert.assertEquals(null, map.get("b"));
    Assert.assertEquals(null, map.get("c"));
    Assert.assertEquals(null, map.get("d"));

    map.put("d", "4");
    map.put("e", "5");
    map.put("f", "6");

    Assert.assertEquals(4, map.size());

    Assert.assertEquals("4", map.get("d"));
    Assert.assertEquals("5", map.get("e"));
    Assert.assertEquals("6", map.get("f"));

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, null);

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    store.beginWindow(windowId);
    map.beginWindow(windowId);
    windowId++;

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, "4");
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, "5");
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, "6");
    SpillableTestUtils.checkValue(store, 0L, "g", ID1, null);

    map.remove("a");
    map.remove("d");
    Assert.assertEquals(null, map.get("a"));
    Assert.assertEquals(null, map.get("b"));
    Assert.assertEquals(null, map.get("c"));
    Assert.assertEquals(null, map.get("d"));
    Assert.assertEquals("5", map.get("e"));
    Assert.assertEquals("6", map.get("f"));
    Assert.assertEquals(null, map.get("g"));

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, "4");
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, "5");
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, "6");
    SpillableTestUtils.checkValue(store, 0L, "g", ID1, null);

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    store.beginWindow(windowId);
    map.beginWindow(windowId);

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, "5");
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, "6");
    SpillableTestUtils.checkValue(store, 0L, "g", ID1, null);

    map.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);
    store.committed(windowId);

    map.teardown();
    store.teardown();
  }

  @Test
  public void multiMapPerBucketTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    multiMapPerBucketTestHelper(store);
  }

  @Test
  public void multiMapPerBucketManagedStateTest()
  {
    multiMapPerBucketTestHelper(testMeta.store);
  }

  public void multiMapPerBucketTestHelper(SpillableStateStore store)
  {
    SerdeStringSlice sss = new SerdeStringSlice();

    SpillableByteMapImpl<String, String> map1 = new SpillableByteMapImpl<>(store, ID1, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice());
    SpillableByteMapImpl<String, String> map2 = new SpillableByteMapImpl<>(store, ID2, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice());

    store.setup(testMeta.operatorContext);
    map1.setup(testMeta.operatorContext);
    map2.setup(testMeta.operatorContext);

    long windowId = 0L;
    store.beginWindow(windowId);
    map1.beginWindow(windowId);
    map2.beginWindow(windowId);
    windowId++;

    map1.put("a", "1");

    Assert.assertEquals("1", map1.get("a"));
    Assert.assertEquals(null, map2.get("a"));

    map2.put("a", "a1");

    Assert.assertEquals("1", map1.get("a"));
    Assert.assertEquals("a1", map2.get("a"));

    map1.put("b", "2");
    map2.put("c", "3");

    Assert.assertEquals("1", map1.get("a"));
    Assert.assertEquals("2", map1.get("b"));

    Assert.assertEquals("a1", map2.get("a"));
    Assert.assertEquals(null, map2.get("b"));
    Assert.assertEquals("3", map2.get("c"));

    map1.endWindow();
    map2.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);

    store.beginWindow(windowId);
    map1.beginWindow(windowId);
    map2.beginWindow(windowId);
    windowId++;

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, "2");

    SpillableTestUtils.checkValue(store, 0L, "a", ID2, "a1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID2, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID2, "3");

    map1.remove("a");

    Assert.assertEquals(null, map1.get("a"));
    Assert.assertEquals("a1", map2.get("a"));

    map1.endWindow();
    map2.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);

    store.beginWindow(windowId);
    map1.beginWindow(windowId);
    map2.beginWindow(windowId);

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "a", ID2, "a1");

    map1.endWindow();
    map2.endWindow();
    store.endWindow();
    store.beforeCheckpoint(windowId);
    store.checkpointed(windowId);

    map1.teardown();
    map2.teardown();
    store.teardown();
  }
}
