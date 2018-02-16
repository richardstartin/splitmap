package com.openkappa.splitmap;

import com.openkappa.splitmap.roaring.DenseMask;
import com.openkappa.splitmap.roaring.Mask;

import java.util.Arrays;
import java.util.function.IntUnaryOperator;

public class SplitMapPageWriter {

  private final IntUnaryOperator hash;
  private final long[] bitmap = new long[1 << 10];
  private final SplitMap splitMap;
  private int currentKey = -1;
  private boolean dirty;

  public SplitMapPageWriter() {
    this(InvertibleHashing::scatter);
  }

  public SplitMapPageWriter(IntUnaryOperator hash) {
    this.splitMap = new SplitMap(hash);
    this.hash = hash;
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
      Mask mask = new DenseMask(bitmap, -1).repairAfterLazy();
      splitMap.insert((short) hash.applyAsInt(currentKey >>> 16),
              mask instanceof DenseMask ? mask.clone() : mask);
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
