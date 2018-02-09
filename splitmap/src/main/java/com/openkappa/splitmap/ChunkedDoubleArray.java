package com.openkappa.splitmap;

import java.util.Arrays;
import java.util.function.DoubleBinaryOperator;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;

public class ChunkedDoubleArray {

  private double[][] pages = new double[1 << 6][];
  private long mask;

  public void write(int index, double[] page) {
    assert index < 64 && index >= 0;
    mask |= (1L << index);
    pages[index] = Arrays.copyOf(page, 1 << 10);
  }

  public double get(int index) {
    assert index < 1 << 16;
    int page = index >>> 10;
    if (null == pages[page]) {
      return 0D;
    }
    return pages[page][index & 0x3FF];
  }

  public boolean writeTo(int index, double[] target) {
    assert index < 64 && index >= 0;
    if ((mask & (1L << index)) == 0) {
      return false;
    }
    System.arraycopy(pages[index >>> 10], 0, target, 0, 1 << 10);
    return true;
  }

  public double[] getPageNoCopy(int index) {
    assert index < 64 && index >= 0;
    return pages[index];
  }

  public double reduce(double initial, DoubleBinaryOperator op) {
    double result = initial;
    long mask = this.mask;
    while (mask != 0) {
      int index = numberOfTrailingZeros(mask);
      double[] page = pages[index];
      for (int i = 0; i < page.length; ++i) {
        result = op.applyAsDouble(result, page[i]);
      }
      mask ^= lowestOneBit(mask);
    }
    return result;
  }

  public void transfer(int index, double[] page) {
    assert index < 64 && index >= 0;
    mask |= (1L << index);
    pages[index] = page;
  }

}
