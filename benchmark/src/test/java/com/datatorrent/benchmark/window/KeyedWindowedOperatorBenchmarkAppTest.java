package com.datatorrent.benchmark.window;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class KeyedWindowedOperatorBenchmarkAppTest extends KeyedWindowedOperatorBenchmarkApp
{
  public static final String basePath = "target/temp";

  @Before
  public void before()
  {
    FileUtil.fullyDelete(new File(basePath));
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
    lc.run(3000000);

    lc.shutdown();
  }

  @Override
  public String getStoreBasePath(Configuration conf)
  {
    return basePath;
  }
}
