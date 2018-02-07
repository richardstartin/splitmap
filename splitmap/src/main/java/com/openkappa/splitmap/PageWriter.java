package com.openkappa.splitmap;

import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.IntUnaryOperator;

public class PageWriter {

  private final IntUnaryOperator hash;
  private final long[] bitmap = new long[1 << 10];
  private final SplitMap splitMap;
  private short currentKey;
  private boolean dirty;

  public PageWriter() {
    this(IntUnaryOperator.identity());
  }

  public PageWriter(IntUnaryOperator hash) {
    this.splitMap = new SplitMap();
    this.hash = hash;
  }

  public void add(int i) {
    short key = (short) (i >>> 16);
    int value = i & 0xFFFF;
    if (key != currentKey) {
      if (Short.compareUnsigned(key, currentKey) < 0) {
        throw new IllegalStateException("append only");
      }
      flush();
      currentKey = key;
    }
    bitmap[value >>> 6] |= (1L << value);
    dirty = true;
  }

  public void flush() {
    if (dirty) {
      Container container = new BitmapContainer(bitmap, -1).repairAfterLazy();
      splitMap.insert((short)hash.applyAsInt(currentKey), container instanceof BitmapContainer ? container.clone() : container);
      clear();
    }
  }

  public SplitMap toSplitMap() {
    flush();
    return splitMap;
  }

  private void clear() {
    Arrays.fill(bitmap, 0L);
    dirty = false;
  }

}
