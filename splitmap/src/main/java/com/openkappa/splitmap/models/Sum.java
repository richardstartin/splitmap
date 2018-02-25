package com.openkappa.splitmap.models;

import com.openkappa.splitmap.*;
import com.openkappa.splitmap.reduction.DoubleReductionContext;
import com.openkappa.splitmap.roaring.Mask;
import com.openkappa.splitmap.roaring.MaskIterator;
import com.openkappa.splitmap.roaring.RunMask;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;

public enum Sum {
  SUM
  ;

  public static <Model>
  ReductionProcedure<Model, Sum, Double, Mask> reducer(PrefixIndex<ChunkedDoubleArray> x1) {
    ReductionContext<Model, Sum, Double> ctx = new DoubleReductionContext<>(x1);
    return ReductionProcedure.mixin(ctx, (key, mask) -> {
      ChunkedDoubleArray x = ctx.readChunk(0, key);
      double result = mask instanceof RunMask
              ? rleSumProduct((RunMask) mask, x)
              : sumProduct(mask, x);
      ctx.contributeDouble(SUM, result, Reduction::add);
    });
  }


  private static double sumProduct(Mask mask, ChunkedDoubleArray x) {
    double result = 0D;
    long pageMask = x.getPageMask();
    MaskIterator it = mask.iterator();
    while (pageMask != 0L) {
      int j = numberOfTrailingZeros(pageMask);
      double[] xPage = x.getPageNoCopy(j);
      int rangeIndex = (j * 1024);
      int next;
      while (it.hasNext() && (next = it.nextAsInt()) < rangeIndex + 1024) {
        result += xPage[next - j * 1024];
      }
      pageMask ^= lowestOneBit(pageMask);
    }
    return result;
  }

  private static double rleSumProduct(RunMask mask, ChunkedDoubleArray x) {
    double result = 0D;
    for (int i = 0; i < mask.numberOfRuns(); ++i) {
      int start = mask.getValue(i) & 0xFFFF;
      int end = start + mask.getLength(i) & 0xFFFF;
      for (int j = start; j < end; ++j) {
        result += x.get(j);
      }
    }
    return result;
  }
}
