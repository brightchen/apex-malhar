package com.datatorrent.benchmark.monitor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceMonitorService
{
  public static ResourceMonitorService create(long period)
  {
    return new ResourceMonitorService(period);
  }

  protected long period;
  protected ScheduledExecutorService executorService;

  protected ResourceMonitorService()
  {
    this(1000);
  }

  protected ResourceMonitorService(long period)
  {
    this.period = period;
  }

  public ResourceMonitorService start()
  {
    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleAtFixedRate(new LogGarbageCollectionTimeTask(period), 0, period, TimeUnit.MILLISECONDS);
    executorService.scheduleAtFixedRate(new LogSystemResourceTask(), 0, period, TimeUnit.MILLISECONDS);
    return this;
  }

  public void shutdownNow()
  {
    executorService.shutdownNow();
  }

  public static class LogGarbageCollectionTimeTask implements Runnable
  {
    private static final Logger logger = LoggerFactory.getLogger(LogGarbageCollectionTimeTask.class);

    protected long period;
    protected long lastGarbageCollectionTime = 0;

    public LogGarbageCollectionTimeTask(long period)
    {
      this.period = period;
    }


    @Override
    public void run()
    {
      long garbageCollectionTime = SystemResourceMonitor.create().getGarbageCollectionTime();
      logger.info("Garbage Collection Time: total: {}; This period: {}; ratio: {}%", garbageCollectionTime,
          garbageCollectionTime - lastGarbageCollectionTime, (garbageCollectionTime - lastGarbageCollectionTime) * 100/period);
      lastGarbageCollectionTime = garbageCollectionTime;
    }
  }

  public static class LogSystemResourceTask implements Runnable
  {
    private static final Logger logger = LoggerFactory.getLogger(LogSystemResourceTask.class);

    @Override
    public void run()
    {
      logger.info("System Resource: {}", SystemResourceMonitor.create(SystemResourceMonitor.allResources).getFormattedSystemResourceUsage());
    }
  }

}
