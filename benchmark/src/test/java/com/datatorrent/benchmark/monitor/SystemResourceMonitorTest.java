package com.datatorrent.benchmark.monitor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemResourceMonitorTest
{
  private static final Logger logger = LoggerFactory.getLogger(SystemResourceMonitorTest.class);

  @Test
  public void testGarbageCollection()
  {
    SystemResourceMonitor monitor = SystemResourceMonitor.create();

    for (int loop = 0; loop < 100; ++loop) {
      for (int i = 0; i < 10000000; ++i) {
        byte[] tmp = new byte[1024];
        tmp = null;
      }
      logger.info("garbage collection cost: {}", monitor.getGarbageCollectionTime());

      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        //ignore
      }
    }
  }

  @Test
  public void testMonitorService()
  {
    ResourceMonitorService.create(1000).start();
    try {
      Thread.sleep(100000);
    } catch (Exception e) {
      //ignore
    }
  }
}
