package com.openkappa.splitmap;

import org.roaringbitmap.Container;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class Aggregation {


  @Test
  public void testProductMomentCorrelationCoefficient() {
    double[] values1 = IntStream.range(0, 1000)
                                .mapToDouble(i -> ThreadLocalRandom.current().nextDouble())
                                .toArray();
    double[] values2 = IntStream.range(0, 1000)
                                .mapToDouble(i -> ThreadLocalRandom.current().nextDouble())
                                .toArray();

    double[] statistics = new double[6];
    for (int i = 0; i < 1000; ++i) {
      double sq = values1[i];
      double sp = values2[i];
      double spp = sp * sp;
      double sqq = sq * sq;
      double spq = sp * sq;
      statistics[0] += sq;
      statistics[1] += sp;
      statistics[2] += spp;
      statistics[3] += sqq;
      statistics[4] += spq;
      statistics[5] += 1;
    }
    double sq = statistics[0];
    double sp = statistics[1];
    double spp = statistics[2];
    double sqq = statistics[3];
    double spq = statistics[4];
    double n = statistics[5];
    double pmccExpected =  (n * spq - sq * sp) / (Math.sqrt((n * spp - sp * sp) * (n * sqq - sq * sq)));

    PrefixIndex<double[]> pi1 = new PrefixIndex<>();
    PrefixIndex<double[]> pi2 = new PrefixIndex<>();
    double[] page1 = new double[1 << 16];
    double[] page2 = new double[1 << 16];

    PageWriter filterWriter = new PageWriter(Hashing::permute);

    int key = 0;
    int multiple = 0;
    for (int k = 0; k < 20; ++k) {
      for (int i = 0; i < 50; ++i) {
        filterWriter.add(key + i);
        page1[i] = values1[i + multiple * 50];
        page2[i] = values2[i + multiple * 50];
      }
      pi1.insert((short)Hashing.permute(key >>> 16), Arrays.copyOf(page1, page1.length));
      pi2.insert((short)Hashing.permute(key >>> 16), Arrays.copyOf(page2, page2.length));
      ++multiple;
      key += 1 << 16;
    }

    SplitMap filter = filterWriter.toSplitMap();
    double[] factors = filter.getIndex()
            .streamUniformPartitions()
            .map(partition -> {
              double[] stats = new double[6];
              partition.forEach((k, c) -> {
                double[] x = pi1.get(k);
                double[] y = pi2.get(k);
                c.forEach((short)0, i -> {
                  double sx = x[i];
                  double sy = y[i];
                  double sxx = sx * sx;
                  double syy = sy * sy;
                  double sxy = sx * sy;
                  stats[0] += sx;
                  stats[1] += sy;
                  stats[2] += sxx;
                  stats[3] += syy;
                  stats[4] += sxy;
                  stats[5] += 1;
                });
              });
              return stats;
            }).reduce(new double[6], (x, y) -> {
              for (int i = 0; i < x.length; ++i) {
                x[i] += y[i];
              }
              return x;
            });

    double sx = factors[0];
    double sy = factors[1];
    double sxx = factors[2];
    double syy = factors[3];
    double sxy = factors[4];
    double n1 = factors[5];
    double pmccActual =  (n1 * sxy - sx * sy) / (Math.sqrt((n1 * syy - sy * sy) * (n1 * sxx - sx * sx)));

    assertEquals(pmccActual, pmccExpected, 1E-5);
  }

  public static IntStream rleRegion() {
    int numRuns = ThreadLocalRandom.current().nextInt(1, 2048);
    int[] runs = createSorted16BitInts(numRuns * 2);
    return IntStream.range(0, numRuns)
            .map(i -> i * 2)
            .mapToObj(i -> IntStream.range(runs[i], runs[i + 1]))
            .flatMapToInt(i -> i);
  }

  public static IntStream sparseRegion() {
    return IntStream.of(createSorted16BitInts(ThreadLocalRandom.current().nextInt(1, 4096)));
  }

  public static IntStream denseRegion() {
    return IntStream.of(createSorted16BitInts(ThreadLocalRandom.current().nextInt(4096, 1 << 16)));
  }

  private static int[] createSorted16BitInts(int howMany) {
    long[] bitset = new long[1 << 10];
    Arrays.fill(bitset, 0L);
    int consumed = 0;
    while (consumed < howMany) {
      int value = ThreadLocalRandom.current().nextInt(1 << 16);
      long bit = (1L << value);
      consumed += 1 - Long.bitCount(bitset[value >>> 6] & bit);
      bitset[value >>> 6] |= bit;
    }
    int[] keys = new int[howMany];
    int prefix = 0;
    int k = 0;
    for (int i = bitset.length - 1; i >= 0; --i) {
      long word = bitset[i];
      while (word != 0) {
        keys[k++] = prefix + Long.numberOfTrailingZeros(word);
        word ^= Long.lowestOneBit(word);
      }
      prefix += 64;
    }
    return keys;
  }

  @Test
  public void aggregationPOC() {

    PrefixIndex<double[]> values1 = new PrefixIndex<>();
    PrefixIndex<double[]> values2 = new PrefixIndex<>();
    PageWriter filterWriter = new PageWriter();
    for (short key = 0; key < 1 << 14; key += 128) {
      values1.insert(key, randomPage());
      values2.insert(key, randomPage());
      short k = key;
      double choice = ThreadLocalRandom.current().nextDouble();
      if (choice > 0.7) {
        rleRegion().map(i -> (k << 16) | i).forEach(filterWriter::add);
      } else if (choice > 0.3) {
        denseRegion().map(i -> (k << 16) | i).forEach(filterWriter::add);
      } else {
        sparseRegion().map(i -> (k << 16) | i).forEach(filterWriter::add);
      }
    }

    PrefixIndex<Container> someFilter = filterWriter.toSplitMap().getIndex();

    double sp = someFilter
            .streamUniformPartitions()
            .parallel()
            .mapToDouble(partition -> {
              double[] closure = new double[1];
              partition.forEach((k, c) -> {
                double[] l = values1.get(k);
                double[] r = values2.get(k);
                c.forEach(k, i -> closure[0] += l[i & 0xFFFF] * r[i & 0xFFFF]);
              });
              return closure[0];
            }).sum();

    assertTrue(sp > 0);

  }

  @Test
  public void aggregationComplexFilterPOC() {

    PrefixIndex<double[]> values1 = new PrefixIndex<>();
    PrefixIndex<double[]> values2 = new PrefixIndex<>();
    PageWriter filterWriter1 = new PageWriter();
    PageWriter filterWriter2 = new PageWriter();
    for (short key = 0; key < 1 << 14; key += 128) {
      values1.insert(key, randomPage());
      values2.insert(key, randomPage());
      short k = key;
      double choice = ThreadLocalRandom.current().nextDouble();
      if (choice > 0.7) {
        rleRegion().map(i -> (k << 16) | i).forEach(filterWriter1::add);
        denseRegion().map(i -> (k << 16) | i).forEach(filterWriter2::add);
      } else if (choice > 0.3) {
        denseRegion().map(i -> (k << 16) | i).forEach(filterWriter1::add);
        sparseRegion().map(i -> (k << 16) | i).forEach(filterWriter2::add);
      } else {
        sparseRegion().map(i -> (k << 16) | i).forEach(filterWriter1::add);
        rleRegion().map(i -> (k << 16) | i).forEach(filterWriter2::add);
      }
    }

    SplitMap someFilter1 = filterWriter1.toSplitMap();
    SplitMap someFilter2 = filterWriter2.toSplitMap();

    double sp = Circuits.evaluate(slice -> slice.get(0).xor(slice.get(1)), someFilter1, someFilter2)
            .getIndex()
            .streamUniformPartitions()
            .parallel()
            .mapToDouble(partition -> {
              double[] closure = new double[1];
              partition.forEach((k, c) -> {
                double[] l = values1.get(k);
                double[] r = values2.get(k);
                c.forEach(k, i -> closure[0] += l[i & 0xFFFF] * r[i & 0xFFFF]);
              });
              return closure[0];
            }).sum();

    assertTrue(sp > 0);

  }

  private double[] randomPage() {
    double[] page = new double[1 << 16]; // naive choice of array length
    for (int i = 0; i < page.length; ++i) {
      page[i] = ThreadLocalRandom.current().nextDouble();
    }
    return page;
  }
}
