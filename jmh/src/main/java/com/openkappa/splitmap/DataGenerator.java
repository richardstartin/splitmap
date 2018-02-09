package com.openkappa.splitmap;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class DataGenerator {


  private static final ThreadLocal<long[]> bits = ThreadLocal.withInitial(() -> new long[1 << 10]);

  public static SplitMap randomSplitmap(int maxKeys, double runniness, double dirtiness) {
    int[] keys = createSorted16BitInts(maxKeys);
    double rleLimit = runniness;
    double denseLimit = runniness + dirtiness;
    SplitMapPageWriter writer = new SplitMapPageWriter();
    IntStream.of(keys)
            .forEach(key -> {
              double choice = ThreadLocalRandom.current().nextDouble();
              final IntStream stream;
              if (choice < rleLimit) {
                stream = rleRegion();
              } else if (choice < denseLimit) {
                stream = denseRegion();
              } else {
                stream = sparseRegion();
              }
              stream.map(i -> (key << 16) | i).filter(i -> i >= 0).forEach(writer::add);
            });
    return writer.toSplitMap();
  }

  public static int[] randomArray(int maxKeys, double runniness, double dirtiness) {
    int[] keys = createSorted16BitInts(maxKeys);
    double rleLimit = runniness;
    double denseLimit = runniness + dirtiness;
    return IntStream.of(keys)
            .flatMap(key -> {
              double choice = ThreadLocalRandom.current().nextDouble();
              final IntStream stream;
              if (choice < rleLimit) {
                stream = rleRegion();
              } else if (choice < denseLimit) {
                stream = denseRegion();
              } else {
                stream = sparseRegion();
              }
              return stream.map(i -> (key << 16) | i).filter(i -> i >= 0);
            }).toArray();
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
    // we can have at most 65536 keys in a RoaringBitmap
    long[] bitset = bits.get();
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
