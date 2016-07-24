package org.apache.apex.malhar.lib.state.spillable;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;

/**
 * Created by tfarkas on 7/17/16.
 */
public class SpillableComplexComponentImplTest
{
  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();

  @Test
  public void simpleIntegrationTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    simpleIntegrationTestHelper(store);
  }

  @Test
  public void simpleIntegrationManagedStateTest()
  {
    simpleIntegrationTestHelper(testMeta.store);
  }
  
  public void simpleIntegrationTestHelper(SpillableStateStore store)
  {
    SpillableComplexComponentImpl sccImpl = new SpillableComplexComponentImpl(store);

    Spillable.SpillableComponent scList =
        (Spillable.SpillableComponent)sccImpl.newSpillableArrayList(0L, new SerdeStringSlice());
    Spillable.SpillableComponent scMap =
        (Spillable.SpillableComponent)sccImpl.newSpillableByteMap(0L, new SerdeStringSlice(), new SerdeStringSlice());

    Assert.assertFalse(scList.isRunning());
    Assert.assertFalse(scMap.isRunning());

    sccImpl.setup(testMeta.operatorContext);

    Assert.assertTrue(scList.isRunning());
    Assert.assertTrue(scMap.isRunning());

    Assert.assertFalse(scList.isInWindow());
    Assert.assertFalse(scMap.isInWindow());

    sccImpl.beginWindow(0L);

    Assert.assertTrue(scList.isInWindow());
    Assert.assertTrue(scMap.isInWindow());

    sccImpl.endWindow();

    Assert.assertFalse(scList.isInWindow());
    Assert.assertFalse(scMap.isInWindow());

    Assert.assertTrue(scList.isRunning());
    Assert.assertTrue(scMap.isRunning());

    sccImpl.teardown();

    Assert.assertFalse(scList.isRunning());
    Assert.assertFalse(scMap.isRunning());
  }
}
