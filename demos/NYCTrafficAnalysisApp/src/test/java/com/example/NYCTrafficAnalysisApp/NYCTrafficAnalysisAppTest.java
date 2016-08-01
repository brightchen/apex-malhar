/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.example.NYCTrafficAnalysisApp;

import org.junit.Test;
import java.net.URI;
import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;

public class NYCTrafficAnalysisAppTest extends NYCTrafficAnalysisApp
{
  private static final Logger logger = LoggerFactory.getLogger(NYCTrafficAnalysisAppTest.class);

  protected long runTime = 60000;

  @Test
  public void test() throws Exception
  {
    String appName = "NYCTrafficAnalysisApp";

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    Configuration conf = new Configuration(false);
    conf.set("dt.application." + appName + ".operator.StoreHDHT.fileStore.basePathPrefix", "target/temp");

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
    lc.runAsync();

    Thread.sleep(runTime);

    lc.shutdown();
  }

  @Override
  protected PubSubWebSocketAppDataQuery createAppDataQuery()
  {
    PubSubWebSocketAppDataQuery query = new PubSubWebSocketAppDataQuery();
    query.setTopic("NYCTrafficAnalysisApp-query");
    try {
      query.setUri(new URI("ws://localhost:9090/pubsub"));
    } catch (URISyntaxException uriE) {
      throw new RuntimeException(uriE);
    }

    return query;
  }

  @Override
  protected PubSubWebSocketAppDataResult createAppDataResult()
  {
    PubSubWebSocketAppDataResult queryResult = new PubSubWebSocketAppDataResult();
    queryResult.setTopic("NYCTrafficAnalysisApp-result");
    try {
      queryResult.setUri(new URI("ws://localhost:9090/pubsub"));
    } catch (URISyntaxException uriE) {
      throw new RuntimeException(uriE);
    }
    return queryResult;
  }
}
