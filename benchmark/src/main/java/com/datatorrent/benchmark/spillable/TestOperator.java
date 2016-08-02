package com.datatorrent.benchmark.spillable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.spillable.SpillableArrayListImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteMapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeLongSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;

import com.datatorrent.api.Context.OperatorContext;
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
  
  public ManagedStateSpillableStateStore store;
  
  public long totalCount = 0;
  public transient long countInWindow;
  public long minWinId = -1;
  public long committedWinId = -1;
  public long windowId;
  
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
    logger.info("checkData(): totalCount: {}; minWinId: {}; committedWinId: {}; curWinId: {}", totalCount, this.minWinId, committedWinId, this.windowId);
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
          logger.error("====data size not equal: window Id: {}; datas size: {}; count: {}", winId, dataSize, count);
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
    multiMap.endWindow();
    windowToCount.put(windowId, countInWindow);
    windowToCount.endWindow();
//    map.endWindow();
//    list.endWindow();
    store.endWindow();
    

    if(windowId % 10 == 0) {
      long startTime = System.currentTimeMillis();
      checkData();
      logger.info("checkData() took {} millis.", System.currentTimeMillis() - startTime);
    }

      
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    //logger.info("===beforeCheckpoint(). windowId: {}; count: {}", windowId, totalCount);
    store.beforeCheckpoint(windowId);
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
    //logger.info("===committed. windowId: {}; count: {}", windowId, count);
  }
  
  public static SpillableByteArrayListMultimapImpl<String, String> createMultimap(SpillableStateStore store)
  {
    return new SpillableByteArrayListMultimapImpl<String, String>(store, ID1, 0L, new SerdeStringSlice(),
        new SerdeStringSlice());
  }
  
  public static SpillableByteMapImpl<String, String> createMap(SpillableStateStore store)
  {
    return new SpillableByteMapImpl<String, String>(store, ID2, 0L, new SerdeStringSlice(),
        new SerdeStringSlice());
  }
  
  public static SpillableByteMapImpl<Long, Long> createWindowToCountMap(SpillableStateStore store)
  {
    return new SpillableByteMapImpl<Long, Long>(store, ID3, 0L, new SerdeLongSlice(),
        new SerdeLongSlice());
  }
}
