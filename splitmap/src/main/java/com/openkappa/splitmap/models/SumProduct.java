package com.openkappa.splitmap.models;

import com.openkappa.splitmap.*;
import com.openkappa.splitmap.reduction.DoubleReductionContext;
import org.roaringbitmap.Container;

public enum SumProduct {
  SUM_PRODUCT;

  public static <Model extends Enum<Model>>
  ReductionProcedure<Model, SumProduct, Double, Container> reducer(PrefixIndex<ChunkedDoubleArray> x1, PrefixIndex<ChunkedDoubleArray> y1) {
    ReductionContext<Model, SumProduct, Double> ctx = new DoubleReductionContext<>(x1, y1);
    return ReductionProcedure.mixin(ctx, (key, mask) -> {
      ChunkedDoubleArray x = ctx.readChunk(0, key);
      ChunkedDoubleArray y = ctx.readChunk(1, key);
      mask.forEach((short) 0, i -> ctx.contributeDouble(SUM_PRODUCT, x.get(i) * y.get(i), Reduction::add));
    });
  }
}
