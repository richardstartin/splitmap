package com.openkappa.splitmap;

import org.roaringbitmap.Container;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.testng.Assert.assertTrue;

public class Aggregation {

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

    double sp = someFilter.streamUniformPartitions()
            .parallel()
            .mapToDouble(partition -> {
              double[] closure =  new double[1];
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
    SplitMap someFilter2 = filterWriter1.toSplitMap();

    double sp = Circuits.evaluate(slice -> slice.get(0).or(slice.get(1)), someFilter1, someFilter2)
            .getIndex()
            .streamUniformPartitions()
            .parallel()
            .mapToDouble(partition -> {
              double[] closure =  new double[1];
              partition.forEach((k, c) -> {
                double[] l = values1.get(k);
                double[] r = values2.get(k);
                c.forEach(k, i -> closure[0] += l[i & 0xFFFF] * r[i & 0xFFFF]);
              });
              return closure[0];
            }).sum();

    assertTrue(sp > 0);
    System.out.println(values1.reduce(0L, arr -> arr.length, (x, y) -> x + y));
    System.out.println(values2.reduce(0L, arr -> arr.length, (x, y) -> x + y));

  }





  private double[] randomPage() {
    double[] page = new double[1 << 16]; // naive choice of array length
    for (int i = 0; i < page.length; ++i) {
      page[i] = ThreadLocalRandom.current().nextDouble();
    }
    return page;
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
}
