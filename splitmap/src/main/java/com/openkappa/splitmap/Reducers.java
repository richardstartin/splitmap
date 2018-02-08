package com.openkappa.splitmap;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;

public class Reducers {

  public static double[] sum(double[] l, double[] r) {
    if (null == l) {
      return r;
    } if (null == r) {
      return l;
    }
    sumRightIntoLeft(l, r);
    return l;
  }

  private static void sumRightIntoLeft(double[] l, double[] r) {
    if (l.length != r.length) {
      throw new IllegalStateException("array lengths must match");
    }
    for (int i = 0; i < l.length && i < r.length; ++i) {
      l[i] += r[i];
    }
  }

  public static final Collector<double[], double[], Double> PMCC = new ProductMomentCorrelationCoefficientCollector();

  private static class ProductMomentCorrelationCoefficientCollector implements Collector<double[], double[], Double> {

    private static Set<Characteristics> CHARACTERISTICS = Set.of(UNORDERED);

      @Override
      public Supplier<double[]> supplier() {
        return () -> new double[6];
      }

      @Override
      public BiConsumer<double[], double[]> accumulator() {
        return Reducers::sumRightIntoLeft;
      }

      @Override
      public BinaryOperator<double[]> combiner() {
        return Reducers::sum;
      }

      @Override
      public Function<double[], Double> finisher() {
        return factors -> {
          double sx = factors[0];
          double sy = factors[1];
          double sxx = factors[2];
          double syy = factors[3];
          double sxy = factors[4];
          double n = factors[5];
          return (n * sxy - sx * sy) / (Math.sqrt((n * syy - sy * sy) * (n * sxx - sx * sx)));
        };
      }

      @Override
      public Set<Characteristics> characteristics() {
        return CHARACTERISTICS;
      }
  }
}
