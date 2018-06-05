package uk.co.openkappa.splitmap.models;

import uk.co.openkappa.splitmap.reduction.DoubleArrayReductionContext;
import org.roaringbitmap.Container;
import org.roaringbitmap.PeekableShortIterator;
import uk.co.openkappa.splitmap.*;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.lang.Long.numberOfTrailingZeros;
import static java.util.stream.Collector.Characteristics.UNORDERED;

public enum VerticalSum {
  ;
  private static final HorizontalSumCollector SUM = new HorizontalSumCollector();

  public static <Model>
  ReductionProcedure<Model, VerticalSum, double[], Container> reducer(PrefixIndex<ChunkedDoubleArray> input) {
    ReductionContext<Model, VerticalSum, double[]> ctx = new DoubleArrayReductionContext<>(1024, VerticalSum::ordinal, input);
    return ReductionProcedure.mixin(ctx, (key, mask) -> {
      ChunkedDoubleArray x = ctx.readChunk(0, key);
      long pageMask = x.getPageMask();
      PeekableShortIterator it = mask.getShortIterator();
      while (pageMask != 0) {
        int pageIndex = numberOfTrailingZeros(pageMask);
        int offset = 1024 * pageIndex;
        double[] page = x.getPageNoCopy(pageIndex);
        if (mask.contains(offset, offset + 1024)) {
          ctx.contribute(page, Reduction::sumRightIntoLeft);
        } else { // slow path
          int next;
          double sum = 0D;
          while (it.hasNext() && (next = it.nextAsInt()) < offset + 1024) {
            sum += page[next - offset];
          }
          ctx.contributeDouble(0, sum, Reduction::add);
        }
        pageMask &= (pageMask - 1);
      }
    });
  }

  public static <Model>
  Collector<ReductionContext<Model, VerticalSum, double[]>, double[], Double> horizontalSum() {
    return (HorizontalSumCollector<Model>) SUM;
  }

  private static class HorizontalSumCollector<Model>
          implements Collector<ReductionContext<Model, VerticalSum, double[]>, double[], Double> {

    private static Set<Characteristics> CHARACTERISTICS = Set.of(UNORDERED);

    @Override
    public Supplier<double[]> supplier() {
      return () -> new double[1024];
    }

    @Override
    public BiConsumer<double[], ReductionContext<Model, VerticalSum, double[]>> accumulator() {
      return (l, r) -> Reduction.sumRightIntoLeft(l, r.getReducedValue());
    }

    @Override
    public BinaryOperator<double[]> combiner() {
      return Reduction::sum;
    }

    @Override
    public Function<double[], Double> finisher() {
      return Reduction::horizontalSum;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return CHARACTERISTICS;
    }
  }
}
