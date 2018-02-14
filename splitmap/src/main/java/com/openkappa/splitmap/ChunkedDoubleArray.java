package com.openkappa.splitmap;

import java.util.Arrays;
import java.util.function.DoubleBinaryOperator;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;

public class ChunkedDoubleArray {

  private double[][] pages = new double[1 << 6][];
  private long mask;

  /**
   * Writes the page to the corresponding index. Copies the page.
   *
   * @param index the index (position % 1024) to write to.
   * @param page  the data.
   */
  public void write(int index, double[] page) {
    assert index < 64 && index >= 0;
    mask |= (1L << index);
    pages[index] = Arrays.copyOf(page, 1 << 10);
  }


  /**
   * Gets the mask for the pages
   * @return the mask
   */
  public long getPageMask() {
    return mask;
  }

  /**
   * Transfer a page without copying it. The caller should not reuse the reference afterwards.
   *
   * @param index the index the page should be placed at.
   * @param page  the data.
   */
  public void transfer(int index, double[] page) {
    assert index < 64 && index >= 0;
    mask |= (1L << index);
    pages[index] = page;
  }

  /**
   * Gets the value at the position. i.e. takes the value at index position % 1024
   * from page Math.floor(position / 1024).
   *
   * @param index the position to get the value from.
   * @return the value.
   */
  public double get(int index) {
    assert index < 1 << 16;
    int page = index >>> 10;
    if (null == pages[page]) {
      return 0D;
    }
    return pages[page][index & 0x3FF];
  }

  /**
   * Writes the contents of a page to a buffer.
   *
   * @param index  the index of the page to read from.
   * @param target the buffer to write onto.
   * @return true if a write succeeded, false otherwise (if the page is missing).
   */
  public boolean writeTo(int index, double[] target) {
    assert index < 64 && index >= 0 && target.length <= 1 << 10;
    if ((mask & (1L << index)) == 0) {
      return false;
    }
    System.arraycopy(pages[index], 0, target, 0, Math.min(target.length, 1 << 10));
    return true;
  }


  /**
   * Gets a page without copying it. Do not modify the contents.
   *
   * @param index the index of the page to get.
   * @return the raw page.
   */
  public double[] getPageNoCopy(int index) {
    assert index < 64 && index >= 0;
    return pages[index];
  }

  /**
   * Reduces the contents of the array to a double.
   *
   * @param initial the initial value (e.g. will be returned if the array is empty)
   * @param op      the reduction operator. Assumed to be associative.
   * @return the reduced value.
   */
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

}
