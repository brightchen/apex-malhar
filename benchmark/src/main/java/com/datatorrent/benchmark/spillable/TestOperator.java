package com.datatorrent.benchmark.spillable;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.spillable.SpillableArrayListImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteMapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeLongSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;

import com.datatorrent.api.Context.OperatorContext;

import com.google.common.collect.Lists;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

public class TestOperator extends BaseOperator implements Operator.CheckpointNotificationListener
{
  public static transient final Logger logger = LoggerFactory.getLogger(TestOperator.class);
  
  public static final byte[] ID1 = new byte[]{(byte)1};
  public static final byte[] ID2 = new byte[]{(byte)2};
  public static final byte[] ID3 = new byte[]{(byte)3};
  
  public SpillableByteArrayListMultimapImpl<String, String> multiMap;
//  protected SpillableByteMapImpl<String, String> map;
//  protected SpillableArrayListImpl<String> list;
  
  public OptimisedStateStore store;
  
  public long totalCount = 0;
  public transient long countInWindow;
  public long minWinId = -1;
  public long committedWinId = -1;
  public long windowId;
  public transient long beginTime;
  public List<SerdeStringSlice> stringSerdes = Lists.newArrayList();
  
  public final transient int delayWindows = 4;
  
  public SpillableByteMapImpl<Long, Long> windowToCount;
  
  public long shutdownCount = -1;
  //this not recovered, why?
  //protected HashSet<String> keys = Sets.newHashSet();
  //protected final String key = "window1";
  
  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
      {
        @Override
        public void process(String tuple)
        {
          processTuple(tuple);
        }
      };
      
  public void processTuple(String tuple)
  {
    if(++totalCount == shutdownCount)
      throw new RuntimeException("Test recovery. count = " + totalCount);
    countInWindow++;
    multiMap.put(""+windowId, tuple);
//    map.put(tuple, tuple);
//    list.add(tuple);
  }
  
  @Override
  public void setup(OperatorContext context)
  {
    beginTime = System.currentTimeMillis();
    super.setup(context);
    if(windowToCount == null) {
      windowToCount = createWindowToCountMap(store);
    }
    if(multiMap == null) {
      multiMap = createMultimap(store);
    }
//    if(map == null)
//      map = createMap(store);
//    if(list == null)
//      list = new SpillableArrayListImpl(3L, ID3, store, new SerdeStringSlice());
    
    store.setup(context);
//    map.setup(context);
    multiMap.setup(context);
//    list.setup(context);
   
//    logger.info("setup(): count: {}; map size: {}", count, multiMap.size());
    checkData();
  }

  public void checkData()
  {
    logger.debug("checkData(): totalCount: {}; minWinId: {}; committedWinId: {}; curWinId: {}", totalCount, minWinId%10000, committedWinId%10000, this.windowId%10000);
    for(long winId = Math.max(committedWinId+1, minWinId); winId < this.windowId; ++winId) {
      Long count = this.windowToCount.get(winId);
      SpillableArrayListImpl<String> datas = (SpillableArrayListImpl<String>)multiMap.get("" + winId);
      if((datas == null && count != null) || (datas != null && count == null)) {
        logger.error("====datas: {}; count: {}", datas, count);
      } else if(datas == null && count == null) {
        logger.error("Both datas and count are null. probably something wrong.");
      } else {
        int dataSize = datas.size();
        if((long)count != (long)dataSize) {
          logger.error("====data size not equal: window Id: {}; datas size: {}; count: {}", winId%10000, dataSize, count);
        } 
      }
    }
  }
  
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void beginWindow(long windowId)
  {
    store.beginWindow(windowId);
    multiMap.beginWindow(windowId);
//    map.beginWindow(windowId);
//    list.beginWindow(windowId);
    if(minWinId < 0) {
      minWinId = windowId;
    }
      
    this.windowId = windowId;
    countInWindow = 0;
  }

  
  /**
   * {@inheritDoc}
   */
  @Override
  public void endWindow()
  {
    long stepStartTime = System.currentTimeMillis();
    
    multiMap.endWindow();
    windowToCount.put(windowId, countInWindow);
    windowToCount.endWindow();
//    map.endWindow();
//    list.endWindow();
    store.endWindow();
    
    //logger.info("save spillable endWindow() took {} millis.", System.currentTimeMillis() - stepStartTime);

    if(windowId % 10 == 0) {
      stepStartTime = System.currentTimeMillis();
      checkData();
      logger.info("checkData() took {} millis.", System.currentTimeMillis() - stepStartTime);
      
      //statistics
      long totalTime = System.currentTimeMillis() - beginTime;
      logger.info("statistics: totalCount: {}; totalTime: {}; average/mill: {}", totalCount, totalTime, totalCount/totalTime);
    }

    stepStartTime = System.currentTimeMillis();
    //save for each window
    store.beforeCheckpoint(windowId);
      
    //it seems the committed never called. call here
    if(windowId - delayWindows >= this.minWinId) {
      committed(windowId - delayWindows );
    }
    
    logger.info("beforeCheckpoint() and committed() took {} millis.", System.currentTimeMillis() - stepStartTime);
    
    stepStartTime = System.currentTimeMillis();
    //clear the serialize buffer
    for(SerdeStringSlice serde : stringSerdes) {
      serde.buffer.reset();
    }
    //logger.info("serde buffer reset() took {} millis.", System.currentTimeMillis() - stepStartTime);
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    long startTime = System.currentTimeMillis();
    store.beforeCheckpoint(windowId);
    logger.info("beforeCheckpoint(). windowId: {}; count: {}; spent time: {}", windowId%10000, totalCount, System.currentTimeMillis() - startTime);
  }
  
  @Override
  public void checkpointed(long windowId)
  {
    //logger.info("===check pointed. windowId: {}; count: {}", windowId, count);
  }

  @Override
  public void committed(long windowId)
  {
    this.committedWinId = windowId;
    store.committed(windowId);
    logger.debug("committed. windowId: {}; count: {}", windowId%10000, totalCount);
  }
  
  public SpillableByteArrayListMultimapImpl<String, String> createMultimap(SpillableStateStore store)
  {
    SerdeStringSlice keySerde = new SerdeStringSlice();
    SerdeStringSlice valueSerde = new SerdeStringSlice();
    stringSerdes.add(keySerde);
    stringSerdes.add(valueSerde);
    return new SpillableByteArrayListMultimapImpl<String, String>(store, ID1, 0L, keySerde, valueSerde);
  }
  
  public SpillableByteMapImpl<String, String> createMap(SpillableStateStore store)
  {
    SerdeStringSlice keySerde = new SerdeStringSlice();
    SerdeStringSlice valueSerde = new SerdeStringSlice();
    stringSerdes.add(keySerde);
    stringSerdes.add(valueSerde);
    return new SpillableByteMapImpl<String, String>(store, ID2, 0L, keySerde, valueSerde);
  }
  
  public SpillableByteMapImpl<Long, Long> createWindowToCountMap(SpillableStateStore store)
  {
    //bucket can only 0 as the default number bucket is 1
    return new SpillableByteMapImpl<Long, Long>(store, ID3, 0L, new SerdeLongSlice(),
        new SerdeLongSlice());
  }
}
