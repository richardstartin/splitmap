package com.openkappa.splitmap.models;

import com.openkappa.splitmap.PrefixIndex;
import com.openkappa.splitmap.ReductionContext;
import com.openkappa.splitmap.ReductionProcedure;
import com.openkappa.splitmap.reduction.DoubleReductionContext;
import org.roaringbitmap.Container;

public enum SumProduct {
  SUM_PRODUCT;

  public static <Model extends Enum<Model>>
  ReductionProcedure<Model, SumProduct, Double, Container> reducer(PrefixIndex<double[]> x1, PrefixIndex<double[]> y1) {
    ReductionContext<Model, SumProduct, Double> ctx = new DoubleReductionContext<>(x1, y1);
    return ReductionProcedure.mixin(ctx, (key, mask) -> {
      double[] x = ctx.readChunk(0, key);
      double[] y = ctx.readChunk(1, key);
      mask.forEach((short) 0, i -> ctx.contributeDouble(SUM_PRODUCT, x[i] * y[i], (l, r) -> l + r));
    });
  }
}
