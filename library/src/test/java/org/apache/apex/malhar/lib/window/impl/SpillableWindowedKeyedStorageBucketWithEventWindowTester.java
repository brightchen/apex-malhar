package org.apache.apex.malhar.lib.window.impl;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.state.managed.BucketsFileSystem;
import org.apache.apex.malhar.lib.state.managed.ManagedStateTestUtils;
import org.apache.apex.malhar.lib.state.managed.MockManagedStateContext;
import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.ObjectKryoSerde;
import org.apache.apex.malhar.lib.utils.serde.SerdeIntSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeMapSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;
import org.apache.apex.malhar.lib.window.Window;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.engine.StreamContext;

public class SpillableWindowedKeyedStorageBucketWithEventWindowTester
{
  protected final int BUCKET_CAPACITY = 1000;
  protected final byte[] identifier = getClass().getSimpleName().getBytes();
  protected final long bucket = System.currentTimeMillis();
  
  protected SpillableWindowedKeyedStorageBucketWithEventWindow<String, Integer> windowedStorage;
  
  @Before
  public void createStorage()
  {
    String applicationPath = "target/" + getClass().getSimpleName() + "/" + "bucket_data";
    
    //The key serde is window; value serde is map
    ManagedStateSpillableStateStore store = new ManagedStateSpillableStateStore();
    //problem: how to decide the buckets? it should be large enough for active event windows
    store.setNumBuckets(BUCKET_CAPACITY);
    ((FileAccessFSImpl)store.getFileAccess()).setBasePath(applicationPath);
    
    windowedStorage = new SpillableWindowedKeyedStorageBucketWithEventWindow<>(store, identifier, bucket, new ObjectKryoSerde(Window.TimeWindow.class), 
        new SerdeMapSlice(new SerdeStringSlice(), new SerdeIntSlice()));
    
    OperatorDeployInfo context = new OperatorDeployInfo();
    StreamContext sc = new StreamContext("1");
    sc.put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 10);
    sc.put(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS, 500);
    context.contextAttributes = sc;
    windowedStorage.setup(context);
  }
  
  @Test
  public void testPut()
  {
    long currentTime = System.currentTimeMillis();
    int tryTime = 3;
    while (tryTime-- > 0) {
      int value = 0;
      for (int beginOffset = 0; beginOffset < 100; ++beginOffset) {
        for (int duration = 1; duration < 100; ++duration) {
          Window window = new Window.TimeWindow(currentTime + beginOffset, duration);
          Spillable.SpillableByteMap<String, Integer> valueMap = windowedStorage.getOrCreate(window);
          valueMap.put(""+value, value);
          ++value;
        }
      }
      windowedStorage.endWindow();
    }
  }
}
