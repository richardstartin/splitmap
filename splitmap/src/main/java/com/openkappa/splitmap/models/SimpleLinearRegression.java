package com.openkappa.splitmap.models;

import com.openkappa.splitmap.*;
import com.openkappa.splitmap.reduction.DoubleArrayReductionContext;
import org.roaringbitmap.Container;
import org.roaringbitmap.PeekableShortIterator;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static com.openkappa.splitmap.ContainerUtils.contains256BitRange;
import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
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
  ReductionProcedure<Model, SimpleLinearRegression, double[], Container> reducer(PrefixIndex<ChunkedDoubleArray> x1,
                                                                                 PrefixIndex<ChunkedDoubleArray> x2) {
    ReductionContext<Model, SimpleLinearRegression, double[]> ctx
            = new DoubleArrayReductionContext<>(PARAMETER_COUNT, x1, x2);
    return ReductionProcedure.mixin(ctx,
            (key, mask) -> {
              ChunkedDoubleArray x = ctx.readChunk(0, key);
              ChunkedDoubleArray y = ctx.readChunk(1, key);
              long pageMask = x.getPageMask() & y.getPageMask();
              PeekableShortIterator it = mask.getShortIterator();
              while (pageMask != 0L) {
                int j = numberOfTrailingZeros(pageMask);
                double[] xPage = x.getPageNoCopy(j);
                double[] yPage = y.getPageNoCopy(j);
                for (int i = 0; i < 4; ++i) {
                  int rangeIndex = (j * 1024) + (i * 256);
                  double sx = 0D;
                  double sy = 0D;
                  double sxx = 0D;
                  double syy = 0D;
                  double sxy = 0D;
                  double n = 0;
                  if (contains256BitRange(mask, rangeIndex)) {
                    for (int k = 0; k < 256; ++k) {
                      double kx = xPage[i * 256 + k];
                      double ky = yPage[i * 256 + k];
                      sx += kx;
                      sy += ky;
                      sxx += kx * kx;
                      syy += ky * ky;
                      sxy += kx * ky;
                    }
                    n = 256;
                    it.advanceIfNeeded((short)(rangeIndex + 256));
                  } else {
                    int next;
                    while (it.hasNext() && (next = it.nextAsInt()) < rangeIndex + 256) {
                      double kx = xPage[next - j * 1024];
                      double ky = yPage[next - j * 1024];
                      sx += kx;
                      sy += ky;
                      sxx += kx * kx;
                      syy += ky * ky;
                      sxy += kx * ky;
                      n += 1;
                    }
                  }
                  ctx.contributeDouble(SX, sx, (l, r) -> l + r);
                  ctx.contributeDouble(SY, sy, (l, r) -> l + r);
                  ctx.contributeDouble(SXX, sxx, (l, r) -> l + r);
                  ctx.contributeDouble(SYY, syy, (l, r) -> l + r);
                  ctx.contributeDouble(SXY, sxy, (l, r) -> l + r);
                  ctx.contributeDouble(N, n, (l, r) -> l + r);
                }
                pageMask ^= lowestOneBit(pageMask);
              }
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
      return (l, r) -> Reduction.sumRightIntoLeft(l, r.getReducedValue());
    }

    @Override
    public BinaryOperator<double[]> combiner() {
      return Reduction::sum;
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
