package com.openkappa.splitmap.models;

import com.openkappa.splitmap.*;
import com.openkappa.splitmap.reduction.DoubleArrayReductionContext;
import org.roaringbitmap.*;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static java.util.stream.Collector.Characteristics.UNORDERED;

public enum Average {
  SUM,
  COUNT;

  public static final int PARAMETER_COUNT = values().length;
  private static final AveragingCollector AVG = new AveragingCollector();

  public static <Model>
  ReductionProcedure<Model, Average, double[], Container> reducer(PrefixIndex<ChunkedDoubleArray> input) {
    ReductionContext<Model, Average, double[]> ctx = new DoubleArrayReductionContext<>(PARAMETER_COUNT, Average::ordinal, input);
    return ReductionProcedure.mixin(ctx, (key, mask) -> {
      ChunkedDoubleArray x = ctx.readChunk(0, key);
      double sum = mask instanceof RunContainer
              ? rleSum((RunContainer) mask, x)
              : mask instanceof ArrayContainer
              ? sum((ArrayContainer) mask, x)
              : pagedSum((BitmapContainer) mask, x);
      ctx.contributeDouble(SUM, sum, Reduction::add);
      ctx.contributeDouble(COUNT, mask.getCardinality(), Reduction::add);
    });
  }

  private static double sum(ArrayContainer mask, ChunkedDoubleArray x) {
    double result = 0D;
    long pageMask = x.getPageMask();
    PeekableShortIterator it = mask.getShortIterator();
    while (pageMask != 0L) {
      int j = numberOfTrailingZeros(pageMask);
      double[] page = x.getPageNoCopy(j);
      int rangeIndex = j * 1024;
      int next;
      while (it.hasNext() && (next = it.nextAsInt()) < rangeIndex + 1024) {
        result += page[next - rangeIndex];
      }
      pageMask ^= lowestOneBit(pageMask);
    }
    return result;
  }

  private static double pagedSum(BitmapContainer mask, ChunkedDoubleArray x) {
    return MaskUtils.pagedSum(mask, x);
  }

  private static double rleSum(RunContainer mask, ChunkedDoubleArray x) {
    return MaskUtils.sum(mask, x);
  }

  public static <Model>
  Collector<ReductionContext<Model, Average, double[]>, double[], Double> collector() {
    return (AveragingCollector<Model>) AVG;
  }

  private static class AveragingCollector<Model>
          implements Collector<ReductionContext<Model, Average, double[]>, double[], Double> {

    private static Set<Characteristics> CHARACTERISTICS = Set.of(UNORDERED);

    @Override
    public Supplier<double[]> supplier() {
      return () -> new double[PARAMETER_COUNT];
    }

    @Override
    public BiConsumer<double[], ReductionContext<Model, Average, double[]>> accumulator() {
      return (l, r) -> Reduction.sumRightIntoLeft(l, r.getReducedValue());
    }

    @Override
    public BinaryOperator<double[]> combiner() {
      return Reduction::sum;
    }

    @Override
    public Function<double[], Double> finisher() {
      return factors -> {
        double sum = factors[SUM.ordinal()];
        double count = factors[COUNT.ordinal()];
        return sum / count;
      };
    }

    @Override
    public Set<Characteristics> characteristics() {
      return CHARACTERISTICS;
    }
  }
}
