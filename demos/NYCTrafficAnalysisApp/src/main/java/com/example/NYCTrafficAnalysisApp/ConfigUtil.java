/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.example.NYCTrafficAnalysisApp;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

/**
 * Created by aayushi on 7/22/16.
 */
public class ConfigUtil
{

  public static final String PROP_GATEWAY_ADDRESS = "dt.gateway.listenAddress";

  public static URI getAppDataQueryPubSubURI(DAG dag, Configuration conf)
  {
    URI uri = URI.create("ws://" + getGatewayAddress(dag, conf) + "/pubsub");
    return uri;
  }

  public static String getGatewayAddress(DAG dag, Configuration conf)
  {
    String gatewayAddress = dag.getValue(Context.DAGContext.GATEWAY_CONNECT_ADDRESS);
    if (gatewayAddress == null) {
      gatewayAddress = conf.get(PROP_GATEWAY_ADDRESS);
    }
    return gatewayAddress;
  }

}