package com.datatorrent.benchmark.spillable;

import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.fileaccess.TFileImpl;

@ApplicationAnnotation(name = "SpillableBenchmarkApp")
public class SpillableBenchmarkApp implements StreamingApplication
{
  protected final String PROP_STORE_PATH = "dt.application.SpillableBenchmarkApp.storeBasePath";
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Create ActiveMQStringSinglePortOutputOperator
    TestInputOperator input = new TestInputOperator();
    input.batchSize = 100;
    input.sleepBetweenBatch = 0;
    input = dag.addOperator("input", input);
    
    TestOperator testOperator = new TestOperator();
    testOperator.store = createStore(conf);
    testOperator.shutdownCount = -1;
    testOperator = dag.addOperator("test", testOperator );

    
    // Connect ports
    dag.addStream("stream", input.output, testOperator.input );//.setLocality(DAG.Locality.CONTAINER_LOCAL);
    //dag.setAttribute(Context.DAGContext.CHECKPOINT_WINDOW_COUNT, 1);  //use normal
    //dag.setAttribute(testOperator, Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 2);    
  }

  
  public ManagedStateSpillableStateStore createStore(Configuration conf)
  {
    String basePath = getStoreBasePath(conf);
    ManagedStateSpillableStateStore store = new ManagedStateSpillableStateStore();
    ((TFileImpl.DTFileImpl)store.getFileAccess()).setBasePath(basePath);
    return store;
  }
  
  public String getStoreBasePath(Configuration conf)
  {
    return Preconditions.checkNotNull(conf.get(PROP_STORE_PATH),
        "base path should be specified in the properties.xml");
  }
}
