package com.openkappa.splitmap;

import java.util.Arrays;
import java.util.function.IntUnaryOperator;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;

public class DoubleArrayPageWriter {

  private final PrefixIndex<ChunkedDoubleArray> index;
  private final double[][] pages = new double[1 << 6][1 << 10];
  private final KeyInvolution involution;

  private long mask = 0;
  private int currentKey = -1;

  public DoubleArrayPageWriter(KeyInvolution involution) {
    this.involution = involution;
    this.index = new PrefixIndex<>();
  }


  public void add(int index, double value) {
    int key = index & 0xFFFF0000;
    int position = index & 0xFFFF;
    if (key != currentKey) {
      if (key < currentKey) {
        throw new IllegalStateException("Must write in ascending order");
      }
      flush();
      currentKey = key;
    }
    pages[position >>> 10][position & 0x3FF] = value;
    mask |= (1L << (position >>> 10));
  }

  public void flush() {
    if (mask != 0) {
      ChunkedDoubleArray storage = new ChunkedDoubleArray();
      while (mask != 0) {
        int page = numberOfTrailingZeros(mask);
        storage.write(page, pages[page]);
        Arrays.fill(pages[page], 0D);
        mask ^= lowestOneBit(mask);
      }
      index.insert(involution.invert((short)(currentKey >>> 16)), storage);
    }
  }


  public PrefixIndex<ChunkedDoubleArray> toIndex() {
    flush();
    return index;
  }
}
