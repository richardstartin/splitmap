package com.openkappa.splitmap.models;

import com.openkappa.splitmap.*;
import com.openkappa.splitmap.reduction.DoubleReductionContext;
import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;
import org.roaringbitmap.PeekableShortIterator;
import org.roaringbitmap.RunContainer;

import static java.lang.Long.numberOfTrailingZeros;

public enum Sum {
  SUM;

  public static <Model>
  ReductionProcedure<Model, Sum, Double, Container> reducer(PrefixIndex<ChunkedDoubleArray> x1) {
    ReductionContext<Model, Sum, Double> ctx = new DoubleReductionContext<>(x1);
    return ReductionProcedure.mixin(ctx, (key, mask) -> {
      ChunkedDoubleArray x = ctx.readChunk(0, key);
      double result = mask instanceof RunContainer
              ? rleSum((RunContainer) mask, x)
              : mask instanceof BitmapContainer
              ? MaskUtils.pagedSum((BitmapContainer) mask, x) : sum(mask, x);
      ctx.contributeDouble(SUM, result, Reduction::add);
    });
  }


  private static double sum(Container mask, ChunkedDoubleArray x) {
    double result = 0D;
    long pageMask = x.getPageMask();
    PeekableShortIterator it = mask.getShortIterator();
    while (pageMask != 0L) {
      int j = numberOfTrailingZeros(pageMask);
      double[] xPage = x.getPageNoCopy(j);
      int rangeIndex = (j * 1024);
      int next;
      while (it.hasNext() && (next = it.nextAsInt()) < rangeIndex + 1024) {
        result += xPage[next - j * 1024];
      }
      pageMask &= (pageMask - 1);
    }
    return result;
  }

  private static double rleSum(RunContainer mask, ChunkedDoubleArray x) {
    return MaskUtils.sum(mask, x);
  }
}
