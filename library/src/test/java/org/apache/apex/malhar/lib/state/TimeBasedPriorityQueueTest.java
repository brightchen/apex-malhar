package org.apache.apex.malhar.lib.state;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.spillable.TimeBasedPriorityQueue;

import com.google.common.collect.Sets;

/**
 * Created by tfarkas on 6/4/16.
 */
public class TimeBasedPriorityQueueTest
{
  @Test
  public void simpleInsertAndRemoveTest()
  {
    TimeBasedPriorityQueue<String> queue = new TimeBasedPriorityQueue<String>();
    queue.upSert("a");
    queue.remove("a");

    overRemoveTest(queue, 1);
  }

  @Test
  public void simpleInsertAndLRURemoveTest()
  {
    TimeBasedPriorityQueue<String> queue = new TimeBasedPriorityQueue<String>();
    queue.upSert("a");

    Set<String> set = queue.removeLRU(1);

    Assert.assertEquals(Sets.newHashSet("a"), set);
  }

  @Test
  public void simpleLRUTest() throws Exception
  {
    TimeBasedPriorityQueue<String> queue = new TimeBasedPriorityQueue<String>();

    queue.upSert("a");
    Thread.sleep(1L);

    queue.upSert("b");
    Thread.sleep(1L);

    queue.upSert("a");

    Set<String> set = queue.removeLRU(1);

    Assert.assertEquals(Sets.newHashSet("b"), set);
  }

  @Test
  public void complexLRUTest() throws Exception
  {
    //0, 3, 6, 9
    //1, 4, 7
    //2, 5, 8

    TimeBasedPriorityQueue<String> queue = new TimeBasedPriorityQueue<String>();

    for (int counter = 0; counter < 10; counter++) {
      String val = "" + counter;

      queue.upSert(val);
      Thread.sleep(1L);
    }

    for (int counter = 0; counter < 10; counter++) {
      if (counter % 3 != 1) {
        continue;
      }

      String val = "" + counter;
      queue.remove(val);
    }

    for (int counter = 0; counter < 10; counter++) {
      if (counter % 3 != 0) {
        continue;
      }

      String val = "" + counter;
      queue.upSert(val);
      Thread.sleep(1L);
    }

    //2, 5, 8, 0, 3, 6, 9

    overRemoveTest(queue, 8);

    Set<String> expiredValues = queue.removeLRU(3);

    Assert.assertEquals(Sets.newHashSet("2", "5", "8"), expiredValues);

    overRemoveTest(queue, 6);

    expiredValues = queue.removeLRU(4);

    Assert.assertEquals(Sets.newHashSet("0", "3", "6", "9"), expiredValues);
  }

  private void overRemoveTest(TimeBasedPriorityQueue<String> queue, int removeCount)
  {
    boolean exceptionThrown = false;

    try {
      queue.removeLRU(removeCount);
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);
  }
}
