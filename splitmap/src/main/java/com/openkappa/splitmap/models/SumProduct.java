package com.openkappa.splitmap.models;

import com.openkappa.splitmap.KeyValueConsumer;
import com.openkappa.splitmap.PrefixIndex;
import com.openkappa.splitmap.ReductionContext;
import com.openkappa.splitmap.reduction.DoubleReductionContext;
import org.roaringbitmap.Container;

public enum SumProduct {
  SUM_PRODUCT;

  public static <Model extends Enum<Model>> KeyValueConsumer<Container> createEvaluation(
          ReductionContext<Model, SumProduct, Double> ctx) {
    return (key, mask) -> {
      double[] x = ctx.readChunk(0, key);
      double[] y = ctx.readChunk(1, key);
      mask.forEach((short) 0, i -> ctx.contributeDouble(SUM_PRODUCT, x[i] * y[i], (l, r) -> l + r));
    };
  }

  public static <Model extends Enum<Model>>
  ReductionContext<Model, SumProduct, Double> createContext(PrefixIndex<double[]> x, PrefixIndex<double[]> y) {
    return new DoubleReductionContext<>(x, y);
  }
}
