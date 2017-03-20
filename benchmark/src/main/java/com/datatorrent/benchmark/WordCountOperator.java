/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.benchmark;

/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;

/**
 * <p>WordCountOperator class.</p>
 *
 * @since 0.3.2
 */
public class WordCountOperator<T> implements Operator
{
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      count++;
    }

  };
  private transient ArrayList<Integer> counts;
  private transient int count;
  private long startmillis;
  private ArrayList<Integer> millis;

  private long totalCount = 0;
  private long totalMillis = 0;
  private long initialTime = 0;
  @Override
  public void endWindow()
  {
    long now = System.currentTimeMillis();
    int spent = (int)(now - startmillis);
    totalCount += count;
    totalMillis += spent;

    counts.add(count);
    millis.add(spent);
    count = 0;

    if (counts.size() % 10 == 0) {
      logger.info("millis = {}", millis);
      logger.info("counts = {}", counts);
      long totalSpent = now - initialTime;
      if (totalMillis > 0 && totalSpent > 0) {
        logger.info("total count: {}; total process millis: {}, rate1: {}; total millis:{}, rate2: {}", totalCount,
            totalMillis, totalCount / totalMillis, totalSpent, totalCount / totalSpent);
      }
      millis.clear();
      counts.clear();
    }
  }

  @Override
  public void teardown()
  {
    logger.info("millis = {}", millis);
    logger.info("counts = {}", counts);
  }

  @Override
  public void beginWindow(long windowId)
  {
    startmillis = System.currentTimeMillis();
  }

  @Override
  public void setup(OperatorContext context)
  {
    counts = new ArrayList<Integer>();
    millis = new ArrayList<Integer>();
    initialTime = System.currentTimeMillis();
  }

  private static final Logger logger = LoggerFactory.getLogger(WordCountOperator.class);
}
