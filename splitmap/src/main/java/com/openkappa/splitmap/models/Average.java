package com.openkappa.splitmap.models;

import com.openkappa.splitmap.*;
import com.openkappa.splitmap.reduction.DoubleArrayReductionContext;
import com.openkappa.splitmap.roaring.*;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static com.openkappa.splitmap.MaskUtils.contains1024BitRange;
import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static java.util.stream.Collector.Characteristics.UNORDERED;

public enum Average {
  SUM,
  COUNT;

  public static final int PARAMETER_COUNT = values().length;
  private static final AveragingCollector AVG = new AveragingCollector();

  public static <Model>
  ReductionProcedure<Model, Average, double[], Mask> reducer(PrefixIndex<ChunkedDoubleArray> input) {
    ReductionContext<Model, Average, double[]> ctx = new DoubleArrayReductionContext<>(PARAMETER_COUNT, Average::ordinal, input);
    return ReductionProcedure.mixin(ctx, (key, mask) -> {
      ChunkedDoubleArray x = ctx.readChunk(0, key);
      double sum = mask instanceof RunMask
              ? rleSum((RunMask) mask, x)
              : mask instanceof SparseMask
              ? sum((SparseMask) mask, x)
              : pagedSum((DenseMask) mask, x);
      ctx.contributeDouble(SUM, sum, Reduction::add);
      ctx.contributeDouble(COUNT, mask.getCardinality(), Reduction::add);
    });
  }

  private static double sum(SparseMask mask, ChunkedDoubleArray x) {
    double result = 0D;
    long pageMask = x.getPageMask();
    MaskIterator it = mask.iterator();
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

  private static double pagedSum(DenseMask mask, ChunkedDoubleArray x) {
    double result = 0D;
    long pageMask = x.getPageMask();
    MaskIterator it = mask.iterator();
    while (pageMask != 0L) {
      int j = numberOfTrailingZeros(pageMask);
      int pageOffset = j * 1024;
      double[] page = x.getPageNoCopy(j);
      if (contains1024BitRange(mask, pageOffset)) {
        for (int k = 0; k < 1024; ++k) {
          result += page[k];
        }
        it.advanceIfNeeded((short) (pageOffset + 1024));
      } else {
        int next;
        while (it.hasNext() && (next = it.nextAsInt()) < pageOffset + 1024) {
          result += page[next - pageOffset];
        }
        }
      pageMask ^= lowestOneBit(pageMask);
    }
    return result;
  }

  private static double rleSum(RunMask mask, ChunkedDoubleArray x) {
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
