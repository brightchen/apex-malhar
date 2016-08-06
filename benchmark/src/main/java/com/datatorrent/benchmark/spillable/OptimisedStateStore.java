package com.datatorrent.benchmark.spillable;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.Bucket;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;

import com.google.common.collect.Maps;

import com.datatorrent.netlet.util.Slice;

public class OptimisedStateStore extends ManagedStateSpillableStateStore
{
  public static transient final Logger logger = LoggerFactory.getLogger(OptimisedStateStore.class);
  
  /**
   * - beforeCheckpoint() and other process method should be in same thread, and no need lock
   */
  @Override
  public void beforeCheckpoint(long windowId)
  {
    Map<Long, Map<Slice, Bucket.BucketedValue>> flashData = Maps.newHashMap();

    for (Bucket bucket : buckets) {
      if (bucket != null && !bucket.isSaved()) {
        Map<Slice, Bucket.BucketedValue> flashDataForBucket = bucket.checkpoint(windowId);
        if (!flashDataForBucket.isEmpty()) {
          flashData.put(bucket.getBucketId(), flashDataForBucket);
        }
        bucket.setSaved();
      }
    }
    if (!flashData.isEmpty()) {
      try {
        getCheckpointManager().save(flashData, operatorContext.getId(), windowId, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
