package com.openkappa.splitmap;

import java.lang.reflect.Array;
import java.util.Arrays;

public class PageWriter {

  private final long[] bitmap = new long[1 << 10];
  private final SplitMap splitMap;
  private short currentKey;
  private int cardinality;
  private boolean dirty;


  public PageWriter() {
    this.splitMap = new SplitMap();
  }

  public void add(int i) {
    short key = (short)(i >>> 16);
    int value = i & 0xFFFF;
    if (key != currentKey) {
      if (Short.compareUnsigned(currentKey, key) > 0) {
        throw new IllegalStateException("Write keys in ascending order");
      }
      flush();
      currentKey = key;
    }
    cardinality += 1 - Long.bitCount(bitmap[value >>> 6] & (1L << value));
    bitmap[value >>> 6] |= (1L << value);
    dirty = true;
  }

  public void flush() {
    if (dirty) {
      splitMap.insert(currentKey, new DirtyRegion(Arrays.copyOf(bitmap, bitmap.length), cardinality));
      clear();
    }
  }

  public SplitMap toSplitMap() {
    flush();
    return splitMap;
  }

  private void clear() {
    cardinality = 0;
    Arrays.fill(bitmap, 0L);
    dirty = false;
  }

}
