package com.openkappa.splitmap;

import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.PeekableShortIterator;
import org.roaringbitmap.RunContainer;

import static java.lang.Long.numberOfTrailingZeros;

public class MaskUtils {

  public static double pagedSum(BitmapContainer mask, ChunkedDoubleArray x) {
    double result = 0D;
    long pageMask = x.getPageMask();
    PeekableShortIterator it = mask.getShortIterator();
    while (pageMask != 0L) {
      int j = numberOfTrailingZeros(pageMask);
      int pageOffset = j * 1024;
      double[] page = x.getPageNoCopy(j);
      if (mask.contains(pageOffset, pageOffset + 1024)) {
        double r1 = 0D;
        double r2 = 0D;
        double r3 = 0D;
        double r4 = 0D;
        for (int k = 0; k < 1024; k += 4) {
          r1 += page[k];
          r2 += page[k + 1];
          r3 += page[k + 2];
          r4 += page[k + 3];
        }
        result += r1 + r2 + r3 + r4;
        it.advanceIfNeeded((short) (pageOffset + 1024));
      } else {
        int next;
        while (it.hasNext() && (next = it.nextAsInt()) < pageOffset + 1024) {
          result += page[next - pageOffset];
        }
      }
      pageMask &= (pageMask - 1);
    }
    return result;
  }

  public static double sum(RunContainer mask, ChunkedDoubleArray x) {
    double result = 0D;
    for (int i = 0; i < mask.numberOfRuns(); ++i) {
      int start = mask.getValue(i) & 0xFFFF;
      int end = start + mask.getLength(i) & 0xFFFF;
      double r1 = 0D, r2 = 0D, r3 = 0D, r4 = 0D;
      int j = start;
      for (; j + 3 < end; j += 4) {
        r1 += x.get(j);
        r2 += x.get(j + 1);
        r3 += x.get(j + 2);
        r4 += x.get(j + 3);
      }
      result += r1 + r2 + r3 + r4;
      for (; j < end; ++j) {
        result += x.get(j);
      }
    }
    return result;
  }
}
