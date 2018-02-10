package com.openkappa.splitmap;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.Container;
import org.roaringbitmap.RunContainer;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class VectorisationBenchmark {

  private static final short ZERO = 0;

  private Container rc;

  private final double[] x = new double[1 << 16];
  private final double[] y = new double[1 << 16];
  private final double[] z = new double[1 << 16];


  @Setup(Level.Trial)
  public void setup() {
    IntStream.range(0, 1 << 16).forEach(i -> x[i] = ThreadLocalRandom.current().nextDouble());
    IntStream.range(0, 1 << 16).forEach(i -> y[i] = x[i] + ThreadLocalRandom.current().nextDouble());;
    rc = RunContainer.full();
  }


  @Benchmark
  public void monomorphicForEach(Blackhole bh) {
    rc.forEach(ZERO, i -> z[i] = x[i] - y[i]);
    bh.consume(z);
  }

  @Benchmark
  public void megamorphicForEach(Blackhole bh) {
    rc.forEach(ZERO, i -> z[i] = x[i] - y[i]);
    bh.consume(z);
    rc.forEach(ZERO, i -> z[i] = z[i] - x[i] * y[i]);
    bh.consume(z);
    rc.forEach(ZERO, i -> z[i] = z[i] + x[i] * y[i]);
    bh.consume(z);
  }

  @Benchmark
  public void bulk1(Blackhole bh) {
    final int cardinality = rc.getCardinality();
    final int offset = 0;
    final int stride = 1 << 9; // magic point where vectorisation kicks in
    int card = cardinality;
    int iteration = 0;
    while (card > 0) {
      int min = iteration * offset;
      int sup = (iteration * offset) + stride;
      // factor in some kind of cost similar to doing .containsRange
      if (rc.contains((short) offset) && rc.contains((short) (offset + stride - 1))) {
        for (int i = min; i < sup; ++i) {
          z[i] = x[i] - y[i];
        }
        card -= stride;
      }
      ++iteration;
    }
    bh.consume(z);
  }

  @Benchmark
  public void bulk3(Blackhole bh) {
    final int cardinality = rc.getCardinality();
    final int offset = 0;
    final int stride = 1 << 9; // magic point where vectorisation kicks in
    int card = cardinality;
    int iteration = 0;
    while (card > 0) {
      int min = iteration * offset;
      int sup = (iteration * offset) + stride;
      // factor in some kind of cost similar to doing .containsRange
      if (rc.contains((short) min) && rc.contains((short) (sup - 1))) {
        for (int i = min; i < sup; ++i) {
          z[i] = x[i] - y[i];
        }
        card -= stride;
      }
      ++iteration;
    }
    bh.consume(z);
    card = cardinality;
    iteration = 0;
    while (card > 0) {
      int min = iteration * offset;
      int sup = (iteration * offset) + stride;
      // factor in some kind of cost similar to doing .containsRange
      if (rc.contains((short) min) && rc.contains((short) (sup - 1))) {
        for (int i = min; i < sup; ++i) {
          z[i] = z[i] - x[i] * y[i];
        }
        card -= stride;
      }
      ++iteration;
    }
    bh.consume(z);
    card = cardinality;
    iteration = 0;
    while (card > 0) {
      int min = iteration * offset;
      int sup = (iteration * offset) + stride;
      // factor in some kind of cost similar to doing .containsRange
      if (rc.contains((short) min) && rc.contains((short) (sup - 1))) {
        for (int i = min; i < sup; ++i) {
          z[i] = z[i] + x[i] * y[i];
        }
        card -= stride;
      }
      ++iteration;
    }
    bh.consume(z);
  }
}
