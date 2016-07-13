/**
 * Put your copyright and license info here.
 */
package com.example.NYCTrafficAnalysisApp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.api.Context;

import com.datatorrent.api.DAG.Locality;

import com.datatorrent.lib.io.ConsoleOutputOperator;


@ApplicationAnnotation(name="NYCTrafficAnalysisApp")
public class NYCTrafficAnalysisApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
      String mySchema = SchemaUtils.jarResourceFileToString("schema.json");

      FileReader reader = dag.addOperator("Reader",  FileReader.class);
      CsvParser parser = dag.addOperator("Parser", CsvParser.class);
      ConsoleOutputOperator consoleOutput = dag.addOperator("Console", ConsoleOutputOperator.class);

      reader.setDirectory("/user/aayushi");
      parser.setSchema(mySchema);

      dag.setOutputPortAttribute(parser.out, Context.PortContext.TUPLE_CLASS, POJOobject.class);
      dag.setInputPortAttribute(consoleOutput.input, Context.PortContext.TUPLE_CLASS, POJOobject.class);


      dag.addStream("ReaderToParser", reader.output, parser.in);
      dag.addStream("ParserToConsole", parser.out, consoleOutput.input);
      //dag.addStream("ReaderToConsole", reader.output, consoleOutput.input);

    //dag.addStream("randomData", randomGenerator.out, cons.input).setLocality(Locality.CONTAINER_LOCAL);
  }
}
