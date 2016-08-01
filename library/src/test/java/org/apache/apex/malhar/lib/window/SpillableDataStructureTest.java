package org.apache.apex.malhar.lib.window;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.spillable.SpillableArrayListImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImplTest;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteMapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeLongSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;

public class SpillableDataStructureTest extends SpillableByteArrayListMultimapImplTest
{
  public static final byte[] ID2 = new byte[]{(byte)2};
  public static final byte[] ID3 = new byte[]{(byte)3};
  
  public static class TestInputOperator extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();
    public long count = 0;
    public int batchSize = 100;
    public int sleepBetweenBatch = 1;

    @Override
    public void emitTuples()
    {
      for (int i = 0; i < batchSize; ++i) {
        output.emit("" + ++count);
      }
      if (sleepBetweenBatch > 0) {
        try {
          Thread.sleep(sleepBetweenBatch);
        } catch (Exception e) {
          //ignore
        }
      }
    }
  }
  
  public static class TestOperator extends BaseOperator implements Operator.CheckpointNotificationListener
  {
    public static transient final Logger logger = LoggerFactory.getLogger(TestOperator.class);
    
    protected SpillableByteArrayListMultimapImpl<String, String> multiMap;
//    protected SpillableByteMapImpl<String, String> map;
//    protected SpillableArrayListImpl<String> list;
    
    protected ManagedStateSpillableStateStore store;
    
    protected long totalCount = 0;
    protected transient long countInWindow;
    protected long minWinId = -1;
    protected long committedWinId = -1;
    protected long windowId;
    
    protected SpillableByteMapImpl<Long, Long> windowToCount;
    
    protected long shutdownCount = -1;
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
//      map.put(tuple, tuple);
//      list.add(tuple);
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
//      if(map == null)
//        map = createMap(store);
//      if(list == null)
//        list = new SpillableArrayListImpl(3L, ID3, store, new SerdeStringSlice());
      
      store.setup(context);
//      map.setup(context);
      multiMap.setup(context);
//      list.setup(context);
     
//      logger.info("setup(): count: {}; map size: {}", count, multiMap.size());
      checkData();
    }

    public void checkData()
    {
      logger.info("checkData(): totalCount: {}; minWinId: {}; committedWinId: {}; curWinId: {}", totalCount, this.minWinId, committedWinId, this.windowId);
      boolean hasErr = false;
      for(long winId = Math.max(committedWinId+1, minWinId); winId < this.windowId; ++winId) {
        Long count = this.windowToCount.get(winId);
        SpillableArrayListImpl<String> datas = (SpillableArrayListImpl<String>)multiMap.get("" + winId);
        if((datas == null && count != null) || (datas != null && count == null)) {
          logger.error("====datas: {}; count: {}", datas, count);
          hasErr = true;
        } else if(datas == null && count == null) {
          logger.error("Both datas and count are null. probably something wrong.");
        } else {
          int dataSize = datas.size();
          if((long)count != (long)dataSize) {
            logger.error("====window Id: {}; datas size: {}; count: {}", winId, dataSize, count);
            hasErr = true;
          } else {
            logger.info("====window Id: {} checked ok, both dataSize and count are {}", winId, count);
          }
        }
        
      }
      Assert.assertTrue(!hasErr);
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void beginWindow(long windowId)
    {
      store.beginWindow(windowId);
      multiMap.beginWindow(windowId);
//      map.beginWindow(windowId);
//      list.beginWindow(windowId);
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
//      map.endWindow();
//      list.endWindow();
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
  }
  
  @Test
  public void recoveryTest() throws Exception
  {
    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    DAG dag = lma.getDAG();

    // Create ActiveMQStringSinglePortOutputOperator
    TestInputOperator input = new TestInputOperator();
    input.batchSize = 1;
    input.sleepBetweenBatch = 100;
    input = dag.addOperator("input", input);
    
    TestOperator testOperator = new TestOperator();
    testOperator.store = testMeta.store;
    testOperator.shutdownCount = 50;
    testOperator = dag.addOperator("test", testOperator );

    
    // Connect ports
    dag.addStream("stream", input.output, testOperator.input );//.setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.setAttribute(Context.DAGContext.CHECKPOINT_WINDOW_COUNT, 1);
    //dag.setAttribute(testOperator, Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 2);
    
    Configuration conf = new Configuration(false);
    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.run(300000);
    //recoveryTest(testMeta.store);
  }
  
  @Test
  public void benchmarkTest() throws Exception
  {
    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    DAG dag = lma.getDAG();

    // Create ActiveMQStringSinglePortOutputOperator
    TestInputOperator input = new TestInputOperator();
    input.batchSize = 100;
    input.sleepBetweenBatch = 1;
    input = dag.addOperator("input", input);
    
    TestOperator testOperator = new TestOperator();
    testOperator.store = testMeta.store;
    testOperator.shutdownCount = -1;
    testOperator = dag.addOperator("test", testOperator );

    
    // Connect ports
    dag.addStream("stream", input.output, testOperator.input );//.setLocality(DAG.Locality.CONTAINER_LOCAL);
    //dag.setAttribute(Context.DAGContext.CHECKPOINT_WINDOW_COUNT, 1);  //use normal
    //dag.setAttribute(testOperator, Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 2);
    
    Configuration conf = new Configuration(false);
    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.run(300000);
    //recoveryTest(testMeta.store);
  }
  
  @Test
  public void testMultimapInternal()
  {
    SpillableStateStore store = testMeta.store;
    SpillableByteArrayListMultimapImpl<String, String> map = createMultimap(store);

    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);
    
    map.beginWindow(1);
    map.put("1", "1");
    map.endWindow();
    store.beforeCheckpoint(1);


    List<String>  list = map.get("1");      //.getFromStore("1");
    //list.setSize(1);
    String value = list.get(0);
    
    Assert.assertTrue("1".equals(value));
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

  //override to ignore the super class test cases
  @Override
  public void simpleMultiKeyTest()
  {
  }

  @Override
  public void simpleMultiKeyManagedStateTest()
  {
  }
}
