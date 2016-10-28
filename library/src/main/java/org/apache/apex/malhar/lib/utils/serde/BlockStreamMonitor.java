package org.apache.apex.malhar.lib.utils.serde;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class BlockStreamMonitor
{
  private static final Logger logger = LoggerFactory.getLogger(BlockStreamMonitor.class);

  public static BlockStreamMonitor INSTANCE = new BlockStreamMonitor();

  private Map<Integer, WindowedBlockStream> streams = Maps.newHashMap();

  public void createdNewStream(WindowedBlockStream stream)
  {
    streams.put(System.identityHashCode(stream), stream);
  }

  public long getAllStreamCapacity()
  {
    long totalCapacity = 0;
    for(WindowedBlockStream stream : streams.values()){
      totalCapacity += stream.capacity();
    }
    return totalCapacity;
  }

  public void logStreamCapacity()
  {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Integer, WindowedBlockStream> entry : streams.entrySet()) {
      if (entry.getValue().capacity() > 0) {
        sb.append(entry.getKey()).append(": ").append(entry.getValue().capacity()).append("\n");
      }
    }
    logger.info("All Stream total capacity: {}", getAllStreamCapacity());
    logger.info("Stream capacity: \n{}", sb.toString());
  }
}
