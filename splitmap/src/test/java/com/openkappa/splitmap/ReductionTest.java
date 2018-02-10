package com.openkappa.splitmap;

import com.openkappa.splitmap.models.Average;
import com.openkappa.splitmap.models.SimpleLinearRegression;
import com.openkappa.splitmap.models.SumProduct;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;


import static org.testng.Assert.assertEquals;

public class ReductionTest {

  private enum InputModel {
    X, Y
  }


  @Test
  public void testProductMomentCorrelationCoefficientFactors() {
    double[] values1 = IntStream.range(0, 1000)
            .mapToDouble(i -> ThreadLocalRandom.current().nextDouble())
            .toArray();
    double[] values2 = IntStream.range(0, 1000)
            .mapToDouble(i -> -ThreadLocalRandom.current().nextDouble())
            .toArray();

    double[] statistics = new double[6];
    for (int i = 0; i < 1000; ++i) {
      double sp = values1[i];
      double sq = values2[i];
      double spp = sp * sp;
      double sqq = sq * sq;
      double spq = sp * sq;
      statistics[0] += sp;
      statistics[1] += sq;
      statistics[2] += spp;
      statistics[3] += sqq;
      statistics[4] += spq;
      statistics[5] += 1;
    }

    DoubleArrayPageWriter writer1 = new DoubleArrayPageWriter(InvertibleHashing::scatter);
    DoubleArrayPageWriter writer2 = new DoubleArrayPageWriter(InvertibleHashing::scatter);
    SplitMapPageWriter filterWriter = new SplitMapPageWriter(InvertibleHashing::scatter);

    int key = 0;
    int multiple = 0;
    for (int k = 0; k < 20; ++k) {
      for (int i = 0; i < 50; ++i) {
        filterWriter.add(key + i);
        writer1.add(key + i, values1[i + multiple * 50]);
        writer2.add(key + i, values2[i + multiple * 50]);
      }
      ++multiple;
      key += 1 << 16;
    }

    PrefixIndex<ChunkedDoubleArray> pi1 = writer1.toIndex();
    PrefixIndex<ChunkedDoubleArray> pi2 = writer2.toIndex();

    SplitMap filter = filterWriter.toSplitMap();
    double[] factors = filter.stream()
            .parallel()
            .map(partition -> partition.reduce(SimpleLinearRegression.<InputModel>reducer(pi1, pi2)).getReducedValue())
            .reduce(Reduction::sum)
            .orElseGet(() -> new double[6]);

    System.out.println(Arrays.toString(statistics) + " -> " + Arrays.toString(factors));
    for (int i = 0; i < factors.length; ++i) {
      assertEquals(factors[i], statistics[i], 1E-5);
    }
  }


  @Test
  public void testProductMomentCorrelationCoefficient() {
    double[] values1 = IntStream.range(0, 1000)
            .mapToDouble(i -> ThreadLocalRandom.current().nextDouble())
            .toArray();
    double[] values2 = IntStream.range(0, 1000)
            .mapToDouble(i -> -ThreadLocalRandom.current().nextDouble())
            .toArray();

    double[] statistics = new double[6];
    for (int i = 0; i < 1000; ++i) {
      double sp = values1[i];
      double sq = values2[i];
      double spp = sp * sp;
      double sqq = sq * sq;
      double spq = sp * sq;
      statistics[0] += sp;
      statistics[1] += sq;
      statistics[2] += spp;
      statistics[3] += sqq;
      statistics[4] += spq;
      statistics[5] += 1;
    }

    double sp = statistics[0];
    double sq = statistics[1];
    double spp = statistics[2];
    double sqq = statistics[3];
    double spq = statistics[4];
    double n = statistics[5];
    double pmccExpected =  (n * spq - sq * sp) / (Math.sqrt((n * spp - sp * sp) * (n * sqq - sq * sq)));

    DoubleArrayPageWriter writer1 = new DoubleArrayPageWriter(InvertibleHashing::scatter);
    DoubleArrayPageWriter writer2 = new DoubleArrayPageWriter(InvertibleHashing::scatter);
    SplitMapPageWriter filterWriter = new SplitMapPageWriter(InvertibleHashing::scatter);

    int key = 0;
    int multiple = 0;
    for (int k = 0; k < 20; ++k) {
      for (int i = 0; i < 50; ++i) {
        filterWriter.add(key + i);
        writer1.add(key + i, values1[i + multiple * 50]);
        writer2.add(key + i, values2[i + multiple * 50]);
      }
      ++multiple;
      key += 1 << 16;
    }

    SplitMap filter = filterWriter.toSplitMap();
    PrefixIndex<ChunkedDoubleArray> pi1 = writer1.toIndex();
    PrefixIndex<ChunkedDoubleArray> pi2 = writer2.toIndex();
    double pmcc = filter.stream()
            .parallel()
            .map(partition -> partition.reduce(SimpleLinearRegression.<InputModel>reducer(pi1, pi2)))
            .collect(SimpleLinearRegression.pmcc());
    assertEquals(pmcc, pmccExpected, 1E-5);
  }


  @Test
  public void average() {
    double[] values1 = IntStream.range(0, 1000)
            .mapToDouble(i -> ThreadLocalRandom.current().nextDouble())
            .toArray();

    double avgExpected = Arrays.stream(values1).sum() / 1000D;


    DoubleArrayPageWriter writer = new DoubleArrayPageWriter(InvertibleHashing::scatter);
    SplitMapPageWriter filterWriter = new SplitMapPageWriter(InvertibleHashing::scatter);
    int key = 0;
    int multiple = 0;
    for (int k = 0; k < 20; ++k) {
      for (int i = 0; i < 50; ++i) {
        filterWriter.add(key + i);
        writer.add(key + i, values1[i + multiple * 50]);
      }
      ++multiple;
      key += 1 << 16;
    }

    PrefixIndex<ChunkedDoubleArray> pi1 = writer.toIndex();

    SplitMap filter = filterWriter.toSplitMap();
    double avg = filter.stream()
            .parallel()
            .map(partition -> partition.reduce(Average.<InputModel>reducer(pi1)))
            .collect(Average.collector());

    assertEquals(avg, avgExpected, 1E-5);
  }


  @Test
  public void sumProduct() {
    double[] values1 = IntStream.range(0, 1000)
            .mapToDouble(i -> ThreadLocalRandom.current().nextDouble())
            .toArray();

    double[] values2 = IntStream.range(0, 1000)
            .mapToDouble(i -> ThreadLocalRandom.current().nextDouble())
            .toArray();
    double spExpected = 0D;
    for (int i = 0; i < 1000; ++i) {
      spExpected += values1[i] * values2[i];
    }

    SplitMapPageWriter filterWriter = new SplitMapPageWriter(InvertibleHashing::scatter);
    DoubleArrayPageWriter writer1 = new DoubleArrayPageWriter(InvertibleHashing::scatter);
    DoubleArrayPageWriter writer2 = new DoubleArrayPageWriter(InvertibleHashing::scatter);

    int key = 0;
    int multiple = 0;
    for (int k = 0; k < 20; ++k) {
      for (int i = 0; i < 50; ++i) {
        filterWriter.add(key + i);
        writer1.add(key + i, values1[i + multiple * 50]);
        writer2.add(key + i, values2[i + multiple * 50]);
      }
      ++multiple;
      key += 1 << 16;
    }

    PrefixIndex<ChunkedDoubleArray> pi1 = writer1.toIndex();
    PrefixIndex<ChunkedDoubleArray> pi2 = writer2.toIndex();
    SplitMap filter = filterWriter.toSplitMap();
    double sp = filter.stream()
            .parallel()
            .mapToDouble(partition -> partition.reduceDouble(SumProduct.<InputModel>reducer(pi1, pi2)))
            .sum();

    assertEquals(sp, spExpected, 1E-5);
  }

}
