package org.apache.apex.malhar.lib.state.managed;

import org.apache.apex.malhar.lib.state.managed.Bucket.BucketedValue;

import com.datatorrent.netlet.util.Slice;

public class BucketValueManager
{
  public static BucketValueManager INSTANCE = new BucketValueManager();

  private static final int CAPACITY = 1000000;

  private BucketValueManager()
  {
    initBucketValues();
  }

  private BucketedValue[] readBucketValues = new BucketedValue[CAPACITY];
  private BucketedValue[] writeBucketValues = new BucketedValue[CAPACITY];
  private int readIndex = 0;
  private int writeIndex = 0;
  protected void initBucketValues()
  {
    for(int i=0; i<CAPACITY; ++i) {
      readBucketValues[i] = new BucketedValue();
      writeBucketValues[i] = new BucketedValue();
    }
  }
  protected BucketedValue allocateBucketValueForRead(long timeBucket, Slice valueSlice)
  {
    BucketedValue bv = readBucketValues[readIndex];
    if (bv.value == null) {
      bv.value = new Slice(valueSlice.buffer, valueSlice.offset, valueSlice.length);
    } else {
      bv.timeBucket = timeBucket;
      bv.value.buffer = valueSlice.buffer;
      bv.value.offset = valueSlice.offset;
      bv.value.length = valueSlice.length;
    }

    if(++readIndex == CAPACITY) {
      readIndex = 0;
    }
    return bv;
  }
  protected BucketedValue allocateBucketValueForWrite(long timeBucket, Slice valueSlice)
  {
    BucketedValue bv = writeBucketValues[writeIndex];
    if (bv.value == null) {
      bv.value = new Slice(valueSlice.buffer, valueSlice.offset, valueSlice.length);
    } else {
      bv.timeBucket = timeBucket;
      bv.value.buffer = valueSlice.buffer;
      bv.value.offset = valueSlice.offset;
      bv.value.length = valueSlice.length;
    }

    if (++writeIndex == CAPACITY) {
      writeIndex = 0;
    }
    return bv;
  }
}
