package com.openkappa.splitmap.models;

import com.openkappa.splitmap.ChunkedDoubleArray;
import com.openkappa.splitmap.PrefixIndex;
import com.openkappa.splitmap.ReductionContext;
import com.openkappa.splitmap.ReductionProcedure;
import com.openkappa.splitmap.reduction.DoubleReductionContext;
import com.openkappa.splitmap.roaring.Mask;
import com.openkappa.splitmap.roaring.MaskIterator;
import com.openkappa.splitmap.roaring.RunMask;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;

public enum SumProduct {
  SUM_PRODUCT;

  public static <Model extends Enum<Model>>
  ReductionProcedure<Model, SumProduct, Double, Mask> reducer(PrefixIndex<ChunkedDoubleArray> x1, PrefixIndex<ChunkedDoubleArray> y1) {
    ReductionContext<Model, SumProduct, Double> ctx = new DoubleReductionContext<>(x1, y1);
    return ReductionProcedure.mixin(ctx, (key, mask) -> {
      ChunkedDoubleArray x = ctx.readChunk(0, key);
      ChunkedDoubleArray y = ctx.readChunk(1, key);
      double result = mask instanceof RunMask
              ? rleSumProduct((RunMask) mask, x, y)
              : sumProduct(mask, x, y);
      ctx.contributeDouble(SUM_PRODUCT, result, (l, r) -> l + r);
    });
  }


  private static double sumProduct(Mask mask, ChunkedDoubleArray x, ChunkedDoubleArray y) {
    double result = 0D;
    long pageMask = x.getPageMask() & y.getPageMask();
    MaskIterator it = mask.iterator();
    while (pageMask != 0L) {
      int j = numberOfTrailingZeros(pageMask);
      double[] xPage = x.getPageNoCopy(j);
      double[] yPage = y.getPageNoCopy(j);
      int rangeIndex = (j * 1024);
      int next;
      while (it.hasNext() && (next = it.nextAsInt()) < rangeIndex + 1024) {
        result = Math.fma(xPage[next - j * 1024], yPage[next - j * 1024], result);
      }
      pageMask ^= lowestOneBit(pageMask);
    }
    return result;
  }

  private static double rleSumProduct(RunMask mask, ChunkedDoubleArray x, ChunkedDoubleArray y) {
    double result = 0D;
    for (int i = 0; i < mask.numberOfRuns(); ++i) {
      int start = mask.getValue(i) & 0xFFFF;
      int end = start + mask.getLength(i) & 0xFFFF;
      for (int j = start; j < end; ++j) {
        result = Math.fma(x.get(j), y.get(j), result);
      }
    }
    return result;
  }
}
