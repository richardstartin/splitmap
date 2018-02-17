package com.openkappa.splitmap;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SLR {

  private double[] x;
  private double[] y;

  @Setup(Level.Trial)
  public void setup() {
    x = IntStream.range(0, 1024).mapToDouble(i -> ThreadLocalRandom.current().nextDouble()).toArray();
    y = IntStream.range(0, 1024).mapToDouble(i -> ThreadLocalRandom.current().nextDouble()).toArray();
  }


  @Benchmark
  public double pmcc() {
    double sx = 0D;
    double sy = 0D;
    double sxx = 0D;
    double syy = 0D;
    double sxy = 0D;
    double n = Math.min(x.length, y.length);
    for (int i = 0; i < x.length && i < y.length; ++i) {
      sx += x[i];
      sy += y[i];
      sxx = Math.fma(x[i], x[i], sxx);
      syy = Math.fma(y[i], y[i], syy);
      sxy = Math.fma(x[i], y[i], sxy);
    }
    return (n * sxy - sx * sy) / (Math.sqrt((n * syy - sy * sy) * (n * sxx - sx * sx)));
  }
}
