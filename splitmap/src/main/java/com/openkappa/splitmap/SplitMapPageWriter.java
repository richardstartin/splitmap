package com.openkappa.splitmap;

import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;

import java.util.Arrays;

public class SplitMapPageWriter {

  private final KeyInvolution involution;
  private final long[] bitmap = new long[1 << 10];
  private final SplitMap splitMap;
  private int currentKey = -1;
  private boolean dirty;

  public SplitMapPageWriter() {
    this(Involutions::reverse);
  }

  public SplitMapPageWriter(KeyInvolution involution) {
    this.splitMap = new SplitMap(involution);
    this.involution = involution;
  }

  public void add(int i) {
    int key = i & 0xFFFF0000;
    int value = i & 0xFFFF;
    if (key != currentKey) {
      if (key < currentKey) {
        throw new IllegalStateException("append only (" + currentKey + " > " + key + ")");
      }
      flush();
      currentKey = key;
    }
    bitmap[value >>> 6] |= (1L << value);
    dirty = true;
  }

  public void flush() {
    if (dirty) {
      Container mask = new BitmapContainer(bitmap, -1).repairAfterLazy();
      splitMap.insert(involution.invert((short)(currentKey >>> 16)),
              mask instanceof BitmapContainer ? mask.clone() : mask);
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
