package org.apache.apex.malhar.lib.state.managed;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.utils.serde.SerializationBuffer;

public class SliceBloomFilterTest
{
  private int loop = 10000000;
  @Test
  public void testBloomFilter()
  {
    testBloomFilter(2);
    testBloomFilter(3);
    testBloomFilter(5);
  }

  public void testBloomFilter(int span)
  {
    SerializationBuffer buffer = SerializationBuffer.READ_BUFFER;

    SliceBloomFilter bf = new SliceBloomFilter(1000, 0.99);

    for (int i = 0; i < loop; i++) {
      if (i % span == 0) {
        buffer.writeInt(i);
        bf.put(buffer.toSlice());
      }
    }
    buffer.getWindowedBlockStream().releaseAllFreeMemory();

    int falsePositive = 0;
    for (int i = 0; i < loop; i++) {
      buffer.writeInt(i);
      if (!bf.mightContain(buffer.toSlice())) {
        Assert.assertTrue(i % span != 0);
      } else {
        // BF says its present
        if (i % span != 0) {
          // But was not there
          falsePositive++;
        }
      }
    }
    buffer.getWindowedBlockStream().releaseAllFreeMemory();
    // Verify false positive prob
//    double falsePositiveProb = (double)falsePositive / loop;
//    Assert.assertTrue(falsePositiveProb <= 0.3);

    for (int i = 0; i < loop; i++) {
      if (i % span == 0) {
        buffer.writeInt(i);
        Assert.assertTrue(bf.mightContain(buffer.toSlice()));
      }
    }
    buffer.getWindowedBlockStream().releaseAllFreeMemory();
  }
}
