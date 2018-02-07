package com.openkappa.splitmap;

import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;

import java.util.Arrays;

public class PageWriter {

  private final long[] bitmap = new long[1 << 10];
  private final SplitMap splitMap;
  private short currentKey;
  private boolean dirty;


  public PageWriter() {
    this.splitMap = new SplitMap();
  }

  public void add(int i) {
    short key = (short) (i >>> 16);
    int value = i & 0xFFFF;
    if (key != currentKey) {
      flush();
      currentKey = key;
    }
    bitmap[value >>> 6] |= (1L << value);
    dirty = true;
  }

  public void flush() {
    if (dirty) {
      Container container = new BitmapContainer(bitmap, -1).repairAfterLazy();
      splitMap.insert(currentKey, container instanceof BitmapContainer ? container.clone() : container);
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
