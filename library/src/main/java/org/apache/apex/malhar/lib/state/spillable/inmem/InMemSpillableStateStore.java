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
package org.apache.apex.malhar.lib.state.spillable.inmem;

import java.util.Map;
import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerializeSlice;
import org.apache.apex.malhar.lib.utils.serde.WindowableBlocksStream;
import org.apache.apex.malhar.lib.utils.serde.WindowableByteStream;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * A simple in memory implementation of a {@link SpillableStateStore} backed by a {@link Map}.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class InMemSpillableStateStore implements SpillableStateStore
{
  private Map<Long, Map<Slice, Slice>> store = Maps.newHashMap();

  //by default, separate key and value into different stream.
  //keyStream and valueStream should created when construction(instead of setup), as the reference will be pass to serialize
  protected WindowableBlocksStream keyStream = new WindowableBlocksStream();
  
  protected WindowableBlocksStream valueStream = new WindowableBlocksStream();
  
  @Override
  public void setup(Context.OperatorContext context)
  {

  }

  @Override
  public void beginWindow(long windowId)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void put(long bucketId, @NotNull Slice key, @NotNull Slice value)
  {
    Map<Slice, Slice> bucket = store.get(bucketId);

    if (bucket == null) {
      bucket = Maps.newHashMap();
      store.put(bucketId, bucket);
    }

    if (key.getClass() == Slice.class) {
      //The hashCode of Slice was not correct, so correct it
      key = new SerializeSlice(key);
    }
    if (value.getClass() == Slice.class) {
      //The hashCode of Slice was not correct, so correct it
      value = new SerializeSlice(value);
    }
    
    bucket.put(key, value);
  }

  @Override
  public Slice getSync(long bucketId, @NotNull Slice key)
  {
    Map<Slice, Slice> bucket = store.get(bucketId);

    if (bucket == null) {
      bucket = Maps.newHashMap();
      store.put(bucketId, bucket);
    }

    if (key.getClass() == Slice.class) {
      //The hashCode of Slice was not correct, so correct it
      key = new SerializeSlice(key);
    }
    return bucket.get(key);
  }

  @Override
  public Future<Slice> getAsync(long bucketId, @NotNull Slice key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void beforeCheckpoint(long l)
  {
  }

  @Override
  public void checkpointed(long l)
  {
  }

  @Override
  public void committed(long l)
  {
  }

  @Override
  public String toString()
  {
    return store.toString();
  }

  @Override
  public WindowableByteStream getKeyStream()
  {
    return keyStream;
  }

  @Override
  public WindowableByteStream getValueStream()
  {
    return valueStream;
  }
}
