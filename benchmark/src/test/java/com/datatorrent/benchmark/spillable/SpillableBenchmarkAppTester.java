package com.datatorrent.benchmark.spillable;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class SpillableBenchmarkAppTester extends SpillableBenchmarkApp
{
  public static final String basePath = "/Users/bright/temp";
  
  @Before 
  public void setup()
  {
    FileUtils.deleteQuietly(new File(basePath));
  }
  
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
    lc.run(300000);

    lc.shutdown();
  }
  
  @Override
  public String getStoreBasePath(Configuration conf)
  {
    return basePath;
  }
}
