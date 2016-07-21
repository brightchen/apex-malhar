/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.window;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.impl.SpillableWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.SpillableWindowedPlainStorage;

import com.datatorrent.api.Attribute;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 * Unit tests for Spillable Windowed Storage
 */
public class SpillableWindowedStorageTest
{
  @Ignore
  @Test
  public void testWindowedPlainStorage()
  {
    SpillableWindowedPlainStorage<Integer> storage = new SpillableWindowedPlainStorage<>();
    Window window1 = new Window.TimeWindow<>(1000, 10);
    Window window2 = new Window.TimeWindow<>(1010, 10);
    Window window3 = new Window.TimeWindow<>(1020, 10);

    storage.setup(new OperatorContextTestHelper.TestIdOperatorContext(1, new Attribute.AttributeMap.DefaultAttributeMap()));
    storage.beginApexWindow(1000);
    storage.put(window1, 1);
    storage.put(window2, 2);
    storage.put(window3, 3);
    storage.endApexWindow();
    storage.beginApexWindow(1001);
    storage.put(window1, 4);
    storage.put(window2, 5);
    storage.endApexWindow();
    storage.beforeCheckpoint(1001);
    storage.checkpointed(1001);
    storage.beginApexWindow(1002);
    storage.put(window1, 6);
    storage.put(window2, 7);
    storage.endApexWindow();

    Assert.assertEquals(6L, storage.get(window1).longValue());
    Assert.assertEquals(7L, storage.get(window2).longValue());
    Assert.assertEquals(3L, storage.get(window3).longValue());

    // simulate recovery
  }

  @Ignore
  @Test
  public void testWindowedKeyedStorage()
  {
    SpillableWindowedKeyedStorage<String, Integer> storage = new SpillableWindowedKeyedStorage<>();
    Window window1 = new Window.TimeWindow<>(1000, 10);
    Window window2 = new Window.TimeWindow<>(1010, 10);
    Window window3 = new Window.TimeWindow<>(1020, 10);

    storage.setup(new OperatorContextTestHelper.TestIdOperatorContext(2));
    storage.beginApexWindow(1000);
    storage.put(window1, "x", 1);
    storage.put(window2, "x", 2);
    storage.put(window3, "x", 3);
    storage.endApexWindow();
    storage.beginApexWindow(1001);
    storage.put(window1, "x", 4);
    storage.put(window2, "x", 5);
    storage.endApexWindow();
    storage.beforeCheckpoint(1001);
    storage.checkpointed(1001);
    storage.beginApexWindow(1002);
    storage.put(window1, "x", 6);
    storage.put(window2, "x", 7);
    storage.endApexWindow();

    Assert.assertEquals(6L, storage.get(window1, "x").longValue());
    Assert.assertEquals(7L, storage.get(window2, "x").longValue());
    Assert.assertEquals(3L, storage.get(window3, "x").longValue());

  }
}
