package com.openkappa.splitmap.models;

import com.openkappa.splitmap.PrefixIndex;
import com.openkappa.splitmap.Reducers;
import com.openkappa.splitmap.ReductionContext;
import com.openkappa.splitmap.ReductionProcedure;
import com.openkappa.splitmap.reduction.DoubleArrayReductionContext;
import org.roaringbitmap.Container;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.util.stream.Collector.Characteristics.UNORDERED;

public enum SimpleLinearRegression {
  SX,
  SY,
  SXX,
  SYY,
  SXY,
  N;

  public static final int PARAMETER_COUNT = values().length;
  private static final ProductMomentCorrelationCoefficientCollector PMCC = new ProductMomentCorrelationCoefficientCollector();

  public static <Model extends Enum<Model>>
  ReductionProcedure<Model, SimpleLinearRegression, double[], Container> reducer(PrefixIndex<double[]> x1, PrefixIndex<double[]> x2) {
    ReductionContext<Model, SimpleLinearRegression, double[]> ctx = new DoubleArrayReductionContext<>(PARAMETER_COUNT, x1, x2);
    return ReductionProcedure.mixin(ctx,
            (key, mask) -> {
              double[] x = ctx.readChunk(0, key);
              double[] y = ctx.readChunk(1, key);
              mask.forEach((short) 0, i -> {
                double sx = x[i];
                double sy = y[i];
                double sxx = sx * sx;
                double syy = sy * sy;
                double sxy = sx * sy;
                ctx.contributeDouble(SX, sx, (l, r) -> l + r);
                ctx.contributeDouble(SY, sy, (l, r) -> l + r);
                ctx.contributeDouble(SXX, sxx, (l, r) -> l + r);
                ctx.contributeDouble(SYY, syy, (l, r) -> l + r);
                ctx.contributeDouble(SXY, sxy, (l, r) -> l + r);
                ctx.contributeDouble(N, 1, (l, r) -> l + r);
              });
            }
    );
  }

  public static <Model extends Enum<Model>>
  Collector<ReductionContext<Model, SimpleLinearRegression, double[]>, double[], Double> pmcc() {
    return (ProductMomentCorrelationCoefficientCollector<Model>) PMCC;
  }

  private static class ProductMomentCorrelationCoefficientCollector<Model extends Enum<Model>>
          implements Collector<ReductionContext<Model, SimpleLinearRegression, double[]>, double[], Double> {

    private static final Set<Characteristics> CHARACTERISTICS = Set.of(UNORDERED);

    @Override
    public Supplier<double[]> supplier() {
      return () -> new double[PARAMETER_COUNT];
    }

    @Override
    public BiConsumer<double[], ReductionContext<Model, SimpleLinearRegression, double[]>> accumulator() {
      return (l, r) -> Reducers.sumRightIntoLeft(l, r.getReducedValue());
    }

    @Override
    public BinaryOperator<double[]> combiner() {
      return Reducers::sum;
    }

    @Override
    public Function<double[], Double> finisher() {
      return factors -> {
        double sx = factors[SX.ordinal()];
        double sy = factors[SY.ordinal()];
        double sxx = factors[SXX.ordinal()];
        double syy = factors[SYY.ordinal()];
        double sxy = factors[SXY.ordinal()];
        double n = factors[N.ordinal()];
        return (n * sxy - sx * sy) / (Math.sqrt((n * syy - sy * sy) * (n * sxx - sx * sx)));
      };
    }

    @Override
    public Set<Characteristics> characteristics() {
      return CHARACTERISTICS;
    }
  }
}
