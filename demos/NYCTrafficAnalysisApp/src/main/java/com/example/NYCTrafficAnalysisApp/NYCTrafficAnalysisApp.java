/**
 * Put your copyright and license info here.
 */
package com.example.NYCTrafficAnalysisApp;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.api.Context;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;

@ApplicationAnnotation(name = "NYCTrafficAnalysisApp")
public class NYCTrafficAnalysisApp implements StreamingApplication
{
  private static final transient Logger logger = LoggerFactory.getLogger(NYCTrafficAnalysisApp.class);

  public String appName = "NYCTrafficAnalysisApp";

  protected String PROP_STORE_PATH;


//  public NYCTrafficAnalysisApp(String appName)
//  {
//    this.appName = appName;
//    PROP_STORE_PATH = "dt.application." + appName + ".operator.StoreHDHT.fileStore.basePathPrefix";
//  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    PROP_STORE_PATH = "dt.application." + appName + ".operator.StoreHDHT.fileStore.basePathPrefix";
      String csvSchema = SchemaUtils.jarResourceFileToString("csvSchema.json");
      String dcSchema = SchemaUtils.jarResourceFileToString("dcSchema.json");

      LineByLineFileInputOperator reader = dag.addOperator("Reader",  LineByLineFileInputOperator.class);
      CsvParser parser = dag.addOperator("Parser", CsvParser.class);
      //ConsoleOutputOperator consoleOutput = dag.addOperator("Console", ConsoleOutputOperator.class);
      DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("DimensionsComputation", DimensionsComputationFlexibleSingleSchemaPOJO.class);
      AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("StoreHDHT", AppDataSingleSchemaDimensionStoreHDHT.class);
      //PubSubWebSocketAppDataQuery query = dag.addOperator("Query", PubSubWebSocketAppDataQuery.class);
      //PubSubWebSocketAppDataResult queryResult = dag.addOperator("QueryResult", PubSubWebSocketAppDataResult.class);

      reader.setDirectory("/user/aayushi/testfiles");
      parser.setSchema(csvSchema);

      // Set operator properties
      // key expression
      {
        Map<String, String> keyToExpression = Maps.newHashMap();
        keyToExpression.put("pickup", "getPickup()");
        keyToExpression.put("cartype", "getCartype()");
        //keyToExpression.put("time", "getTime()");
        dimensions.setKeyToExpression(keyToExpression);
      }

      // aggregate expression
      {
        Map<String, String> aggregateToExpression = Maps.newHashMap();
        aggregateToExpression.put("fare", "getFare()");
        dimensions.setAggregateToExpression(aggregateToExpression);
      }

      dimensions.setConfigurationSchemaJSON(dcSchema);

      //store
      String basePath = Preconditions.checkNotNull(conf.get(PROP_STORE_PATH),
        "base path should be specified in the properties.xml");
      TFileImpl hdsFile = new TFileImpl.DTFileImpl();
      basePath += System.currentTimeMillis();
      hdsFile.setBasePath(basePath);

      store.setFileStore(hdsFile);
      dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
      new BasicCounters.LongAggregator<MutableLong>());
      store.setConfigurationSchemaJSON(dcSchema);

      //query
      PubSubWebSocketAppDataQuery query = createAppDataQuery();
      URI queryUri = ConfigUtil.getAppDataQueryPubSubURI(dag, conf);
      logger.info("QueryUri: {}", queryUri);
      query.setUri(queryUri);
      //use the EmbeddableQueryInfoProvider instead to get rid of the problem of query schema when latency is very long
      store.setEmbeddableQueryInfoProvider(query);

      //query result
      PubSubWebSocketAppDataResult queryResult = createAppDataResult();
      queryResult.setUri(queryUri);

      // Set remaining dag options
      dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
      new BasicCounters.LongAggregator<MutableLong>());

      dag.setOutputPortAttribute(parser.out, Context.PortContext.TUPLE_CLASS, POJOobject.class);
      dag.setInputPortAttribute(dimensions.input, Context.PortContext.TUPLE_CLASS, POJOobject.class);
      //dag.setInputPortAttribute(consoleOutput.input, Context.PortContext.TUPLE_CLASS, POJOobject.class);

      dag.addStream("FileInputToParser", reader.output, parser.in);
      dag.addStream("ParserToDC", parser.out, dimensions.input);
      dag.addStream("DimensionalStreamToStore", dimensions.output, store.input);
      dag.addStream("QueryResult", store.queryResult, queryResult.input);

      //dag.addStream("ParserToConsole", parser.out, consoleOutput.input);
      //dag.addStream("FileInputToConsole", reader.output, consoleOutput.input);
      //dag.addStream("randomData", randomGenerator.out, cons.input).setLocality(Locality.CONTAINER_LOCAL);
  }

  protected PubSubWebSocketAppDataQuery createAppDataQuery()
  {
    return new PubSubWebSocketAppDataQuery();
  }

  protected PubSubWebSocketAppDataResult createAppDataResult()
  {
    return new PubSubWebSocketAppDataResult();
  }

}
