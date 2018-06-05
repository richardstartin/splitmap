package uk.co.openkappa.splitmap.models;

import uk.co.openkappa.splitmap.reduction.DoubleArrayReductionContext;
import org.roaringbitmap.Container;
import org.roaringbitmap.PeekableShortIterator;
import org.roaringbitmap.RunContainer;
import uk.co.openkappa.splitmap.*;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

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

  public static <Model>
  ReductionProcedure<Model, SimpleLinearRegression, double[], Container> reducer(PrefixIndex<ChunkedDoubleArray> x1,
                                                                                 PrefixIndex<ChunkedDoubleArray> x2) {
    ReductionContext<Model, SimpleLinearRegression, double[]> ctx
            = new DoubleArrayReductionContext<>(PARAMETER_COUNT, SimpleLinearRegression::ordinal, x1, x2);
    return ReductionProcedure.mixin(ctx,
            (key, mask) -> {
              ChunkedDoubleArray x = ctx.readChunk(0, key);
              ChunkedDoubleArray y = ctx.readChunk(1, key);
              if (mask instanceof RunContainer) {
                computeRLE((RunContainer) mask, x, y, ctx);
              } else {
                compute(mask, x, y, ctx);
              }
            }
    );
  }


  private static void compute(Container mask,
                              ChunkedDoubleArray x,
                              ChunkedDoubleArray y,
                              ReductionContext<?, SimpleLinearRegression, double[]> ctx) {
    PeekableShortIterator it = mask.getShortIterator();
    long pageMask = x.getPageMask() & y.getPageMask();
    double sx = 0D;
    double sy = 0D;
    double sxx = 0D;
    double syy = 0D;
    double sxy = 0D;
    while (pageMask != 0L) {
      int j = numberOfTrailingZeros(pageMask);
      double[] xPage = x.getPageNoCopy(j);
      double[] yPage = y.getPageNoCopy(j);
      int rangeIndex = (j * 1024);
      int next;
      while (it.hasNext() && (next = it.nextAsInt()) < rangeIndex + 1024) {
        double kx = xPage[next - rangeIndex];
        double ky = yPage[next - rangeIndex];
        sx += kx;
        sy += ky;
        sxx = Math.fma(kx, kx, sxx);
        syy = Math.fma(ky, ky, syy);
        sxy = Math.fma(kx, ky, sxy);
      }
      pageMask ^= lowestOneBit(pageMask);
    }
    ctx.contributeDouble(SX, sx, Reduction::add);
    ctx.contributeDouble(SY, sy, Reduction::add);
    ctx.contributeDouble(SXX, sxx, Reduction::add);
    ctx.contributeDouble(SYY, syy, Reduction::add);
    ctx.contributeDouble(SXY, sxy, Reduction::add);
    ctx.contributeDouble(N, mask.getCardinality(), Reduction::add);
  }


  private static void computeRLE(RunContainer mask,
                                 ChunkedDoubleArray x,
                                 ChunkedDoubleArray y,
                                 ReductionContext<?, SimpleLinearRegression, double[]> ctx) {
    double sx = 0D;
    double sy = 0D;
    double sxx = 0D;
    double syy = 0D;
    double sxy = 0D;
    for (int i = 0; i < mask.numberOfRuns(); ++i) {
      int start = mask.getValue(i) & 0xFFFF;
      int end = start + mask.getLength(i) & 0xFFFF;
      for (int j = start; j < end; ++j) {
        double kx = x.get(j);
        double ky = y.get(j);
        sx += kx;
        sy += ky;
        sxx = Math.fma(kx, kx, sxx);
        syy = Math.fma(ky, ky, syy);
        sxy = Math.fma(kx, ky, sxy);
      }
    }
    ctx.contributeDouble(SX, sx, Reduction::add);
    ctx.contributeDouble(SY, sy, Reduction::add);
    ctx.contributeDouble(SXX, sxx, Reduction::add);
    ctx.contributeDouble(SYY, syy, Reduction::add);
    ctx.contributeDouble(SXY, sxy, Reduction::add);
    ctx.contributeDouble(N, mask.getCardinality(), Reduction::add);
  }

  public static <Model>
  Collector<ReductionContext<Model, SimpleLinearRegression, double[]>, double[], Double> pmcc() {
    return (ProductMomentCorrelationCoefficientCollector<Model>) PMCC;
  }

  private static class ProductMomentCorrelationCoefficientCollector<Model>
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
