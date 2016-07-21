package org.apache.apex.malhar.lib.state.spillable;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeIntSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;

import com.google.common.collect.Lists;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 7/17/16.
 */
public class SpillableByteArrayListMultimapImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};

  @Test
  public void simpleMultiKeyTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    SpillableByteArrayListMultimapImpl<String, String> map =
        new SpillableByteArrayListMultimapImpl<String, String>(store, ID1, 0L, new SerdeStringSlice(),
        new SerdeStringSlice());

    long nextWindowId = simpleMultiKeyTestHelper(store, map, "a", 0L);
  }

  public long simpleMultiKeyTestHelper(InMemSpillableStateStore store,
      SpillableByteArrayListMultimapImpl<String, String> map, String key, long nextWindowId)
  {
    SerdeStringSlice serdeString = new SerdeStringSlice();
    SerdeIntSlice serdeInt = new SerdeIntSlice();

    Slice keySlice = serdeString.serialize(key);
    byte[] keyBytes = SliceUtils.concatenate(ID1, keySlice.toByteArray());

    map.beginWindow(nextWindowId);
    nextWindowId++;

    Assert.assertNull(map.get(key));

    map.put(key, "a");

    List<String> list1 = map.get(key);
    Assert.assertEquals(1, list1.size());

    Assert.assertEquals("a", list1.get(0));

    list1.addAll(Lists.newArrayList("a", "b", "c", "d", "e", "f", "g"));

    Assert.assertEquals(8, list1.size());

    Assert.assertEquals("a", list1.get(0));
    Assert.assertEquals("a", list1.get(1));
    Assert.assertEquals("b", list1.get(2));
    Assert.assertEquals("c", list1.get(3));
    Assert.assertEquals("d", list1.get(4));
    Assert.assertEquals("e", list1.get(5));
    Assert.assertEquals("f", list1.get(6));
    Assert.assertEquals("g", list1.get(7));

    map.endWindow();

    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, SpillableByteArrayListMultimapImpl.SIZE_KEY_SUFFIX), 8, 0, serdeInt);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "a", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "a", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "b", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "c", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "d", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "e", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "f", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "g", 0, serdeString);

    map.beginWindow(nextWindowId);
    nextWindowId++;

    List<String> list2 = map.get(key);

    Assert.assertEquals(8, list2.size());

    Assert.assertEquals("a", list2.get(0));
    Assert.assertEquals("a", list2.get(1));
    Assert.assertEquals("b", list2.get(2));
    Assert.assertEquals("c", list2.get(3));
    Assert.assertEquals("d", list2.get(4));
    Assert.assertEquals("e", list2.get(5));
    Assert.assertEquals("f", list2.get(6));
    Assert.assertEquals("g", list2.get(7));

    list2.add("tt");
    list2.add("ab");
    list2.add("99");
    list2.add("oo");

    Assert.assertEquals("tt", list2.get(8));
    Assert.assertEquals("ab", list2.get(9));
    Assert.assertEquals("99", list2.get(10));
    Assert.assertEquals("oo", list2.get(11));

    map.endWindow();

    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, SpillableByteArrayListMultimapImpl.SIZE_KEY_SUFFIX), 8, 0, serdeInt);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "a", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "a", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "b", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "c", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "d", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "e", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "f", 0, serdeString);
    SpillableTestUtils.checkValue(store, 0L,
        SliceUtils.concatenate(keyBytes, GPOUtils.serializeInt(0)), "g", 0, serdeString);

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.<String>newArrayList("a", "a", "b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.<String>newArrayList("c", "d", "e"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.<String>newArrayList("f", "g", "tt"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.<String>newArrayList("ab", "99", "oo"));

    map.beginWindow(nextWindowId);
    nextWindowId++;
/*
    list.set(1, "111");
    list.set(3, "222");
    list.set(5, "333");
    list.set(11, "444");

    Assert.assertEquals("a", list.get(0));
    Assert.assertEquals("111", list.get(1));
    Assert.assertEquals("b", list.get(2));
    Assert.assertEquals("222", list.get(3));
    Assert.assertEquals("d", list.get(4));
    Assert.assertEquals("333", list.get(5));
    Assert.assertEquals("f", list.get(6));
    Assert.assertEquals("g", list.get(7));
    Assert.assertEquals("tt", list.get(8));
    Assert.assertEquals("ab", list.get(9));
    Assert.assertEquals("99", list.get(10));
    Assert.assertEquals("444", list.get(11));

    map.endWindow();

    SpillableTestUtils.checkValue(store, 0L, ID1, 0, Lists.<String>newArrayList("a", "111", "b"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 1, Lists.<String>newArrayList("222", "d", "333"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 2, Lists.<String>newArrayList("f", "g", "tt"));
    SpillableTestUtils.checkValue(store, 0L, ID1, 3, Lists.<String>newArrayList("ab", "99", "444"));*/

    return nextWindowId;
  }
}
