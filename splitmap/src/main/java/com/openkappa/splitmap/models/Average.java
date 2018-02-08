package com.openkappa.splitmap.models;

import com.openkappa.splitmap.KeyValueConsumer;
import com.openkappa.splitmap.PrefixIndex;
import com.openkappa.splitmap.Reducers;
import com.openkappa.splitmap.ReductionContext;
import com.openkappa.splitmap.reduction.DoubleArrayReductionContext;
import org.roaringbitmap.Container;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.util.stream.Collector.Characteristics.UNORDERED;

public enum Average {
  SUM,
  COUNT;

  public static final int PARAMETER_COUNT = values().length;

  public static <Model extends Enum<Model>> KeyValueConsumer<Container> createEvaluation(
          ReductionContext<Model, Average, double[]> ctx) {
    return (key, mask) -> {
      double[] x = ctx.readChunk(0, key);
      mask.forEach((short) 0, i -> {
        ctx.contributeDouble(SUM, x[i], (l, r) -> l + r);
        ctx.contributeDouble(COUNT, 1, (l, r) -> l + r);
      });
    };
  }

  public static <Model extends Enum<Model>>
  ReductionContext<Model, Average, double[]> createContext(PrefixIndex<double[]> x) {
    return new DoubleArrayReductionContext<>(PARAMETER_COUNT, x);
  }

  public static final Collector<ReductionContext<?, Average, double[]>, double[], Double> AVG = new AveragingCollector();

  private static class AveragingCollector<Model extends Enum<Model>>
          implements Collector<ReductionContext<Model, Average, double[]>, double[], Double> {

    private static Set<Characteristics> CHARACTERISTICS = Set.of(UNORDERED);

    @Override
    public Supplier<double[]> supplier() {
      return () -> new double[PARAMETER_COUNT];
    }

    @Override
    public BiConsumer<double[], ReductionContext<Model, Average, double[]>> accumulator() {
      return (l, r) -> Reducers.sumRightIntoLeft(l, r.getReducedValue());
    }

    @Override
    public BinaryOperator<double[]> combiner() {
      return Reducers::sum;
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
