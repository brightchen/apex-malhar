package com.datatorrent.benchmark.spillable;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class SpillableBenchmarkAppTester extends SpillableBenchmarkApp
{
  public static final String basePath = "target/temp";
  @Test
  public void test() throws Exception
  {
    Configuration conf = new Configuration(false);
    
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    super.populateDAG(dag, conf);

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.run(60000);

    lc.shutdown();
  }
  
  @Override
  public String getStoreBasePath(Configuration conf)
  {
    return basePath;
  }
}
