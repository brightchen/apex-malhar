package org.apache.apex.malhar.lib.state.spillable;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.Bucket;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringWithLVBuffer;

import com.google.common.collect.Maps;

import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.netlet.util.Slice;

public class SpillableDSBenchmarkTest  extends SpillableByteArrayListMultimapImplTest
{
  public static transient final Logger logger = LoggerFactory.getLogger(SpillableDSBenchmarkTest.class);
  
  public static class OptimisedStateStore extends ManagedStateSpillableStateStore
  {
    protected long windowId;
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      this.windowId = windowId;
    }
    
    @Override
    public void endWindow()
    {
      super.endWindow();
      beforeCheckpoint(this.windowId);
    }
    
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
  
  @Test
  public void testSpillableMutimap()
  {
    testSpillableMutimap(true);
    logger.info("=====================above use LVBuffer, below not=====================");
    testSpillableMutimap(false);
  }
  
  public void testSpillableMutimap(boolean useLvBuffer)
  {
    byte[] ID1 = new byte[]{(byte)1};
    OptimisedStateStore store = new OptimisedStateStore();
    ((TFileImpl.DTFileImpl)store.getFileAccess()).setBasePath("target/temp");

    
    SerdeStringSlice keySerde;
    SerdeStringSlice valueSerde;
    
    if(useLvBuffer) {
      keySerde = new SerdeStringWithLVBuffer();  
      valueSerde = new SerdeStringWithLVBuffer();  
    } else {
      keySerde = new SerdeStringSlice();
      valueSerde = new SerdeStringSlice();
    }



    
    SpillableByteArrayListMultimapImpl<String, String> multiMap = new SpillableByteArrayListMultimapImpl<String, String>(store, ID1, 0L, keySerde, valueSerde);
    

    store.setup(testMeta.operatorContext);
    multiMap.setup(testMeta.operatorContext);
    
    int loopCount = 100000;
    String[] strs = new String[]{"123", "45678", "abcdef", "dfaqecdgr"};
    
    long startTime = System.currentTimeMillis();
    long key = 0;
    
    long windowId = 0;
    store.beginWindow(++windowId);
    multiMap.beginWindow(windowId);
    
    for(int i=0; i<loopCount; ++i) {
      for(String str : strs) {
        multiMap.put("" + key, str);
      }

      if(i % 1000 == 0)
      {
        multiMap.endWindow();
        store.endWindow();
        
        //NOTES: it will great impact the performance if the size of buffer is too large
        if(useLvBuffer) {
          //clear the buffer
          ((SerdeStringWithLVBuffer)keySerde).buffer.reset();
          ((SerdeStringWithLVBuffer)valueSerde).buffer.reset();
        }
        
        ++key;
        if(i % 10000 == 0) {
          long spentTime = System.currentTimeMillis() - startTime;
          logger.info("Spent {} mills for {} operation. average: {}", spentTime, strs.length*i, strs.length*i/spentTime);
        }
        
        //next window
        store.beginWindow(++windowId);
        multiMap.beginWindow(windowId);
      }
    }
    long spentTime = System.currentTimeMillis() - startTime;
    
    logger.info("Spent {} mills for {} operation. average: {}", spentTime, strs.length*loopCount, strs.length*loopCount/spentTime);
  }
  
  @Override
  public void simpleMultiKeyManagedStateTest()
  {
  }
  
  @Override
  public void recoveryTestWithManagedState()
  {
  }
}
