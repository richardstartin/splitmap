package com.openkappa.splitmap;

import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.Container;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.LongBinaryOperator;
import java.util.stream.IntStream;

import static com.openkappa.splitmap.PrefixIndex.PARTITIONS;
import static com.openkappa.splitmap.PrefixIndex.PARTITION_SIZE;
import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static java.util.stream.Collectors.toList;

public class Circuits {

  private static final Container EMPTY = new ArrayContainer();
  private static final ThreadLocal<long[]> TEMPORARY_KEYS = ThreadLocal.withInitial(() -> new long[1 << 10]);

  public static SplitMap evaluateIfKeysIntersect(Function<List<Container>, Container> circuit, SplitMap... splitMaps) {
    PrefixIndex<Container>[] indices = Arrays.stream(splitMaps).map(SplitMap::getIndex).toArray(PrefixIndex[]::new);
    return new SplitMap(groupByIntersectingKeys(EMPTY, indices)
            .streamUniformPartitions()
            .parallel()
            .collect(new IndexAggregator<>(circuit)));
  }


  public static SplitMap evaluate(Function<List<Container>, Container> circuit, SplitMap... splitMaps) {
    PrefixIndex<Container>[] indices = Arrays.stream(splitMaps).map(SplitMap::getIndex).toArray(PrefixIndex[]::new);
    return new SplitMap(groupByKey(EMPTY, indices)
            .streamUniformPartitions()
            .parallel()
            .collect(new IndexAggregator<>(circuit)));
  }

  static <T> PrefixIndex<List<T>> groupByKey(T defaultValue, PrefixIndex<T>... indices) {
    return groupByKey((x, y) -> x | y, 0L, defaultValue, indices);
  }

  static <T> PrefixIndex<List<T>> groupByIntersectingKeys(T defaultValue, PrefixIndex<T>... indices) {
    return groupByKey((x, y) -> x & y, -1L, defaultValue, indices);
  }

  private static <T> PrefixIndex<List<T>> groupByKey(LongBinaryOperator op,
                                                     long identity,
                                                     T defaultValue,
                                                     PrefixIndex<T>... indices) {
    PrefixIndex<List<T>> grouped = new PrefixIndex<>(TEMPORARY_KEYS.get());
    List<T> prototype = IntStream.range(0, indices.length).mapToObj(i -> defaultValue).collect(toList());
    IntStream.range(0, PARTITIONS)
            .parallel()
            .forEach(p -> {
              for (int i = PARTITION_SIZE * p; i < PARTITION_SIZE * (p + 1); ++i) {
                long word = identity;
                for (PrefixIndex<T> index : indices) {
                  word = index.computeKeyWord(i, word, op);
                }
                grouped.transferChunk(i, word, null);
                if (word != 0) {
                  List<T>[] chunk = new List[Long.SIZE];
                  int k = 0;
                  for (PrefixIndex<T> index : indices) {
                    T[] column = index.getChunkNoCopy(i);
                    if (null == column) {
                      continue;
                    }
                    long mask = word;
                    while (mask != 0) {
                      int j = numberOfTrailingZeros(mask);
                      if (null != column[j]) {
                        if (null == chunk[j]) {
                          chunk[j] = new ArrayList<>(prototype);
                        }
                        chunk[j].set(k, column[j]);
                      }
                      mask ^= lowestOneBit(mask);
                    }
                    ++k;
                  }
                  grouped.transferChunk(i, word, chunk);
                }
              }
            });
    return grouped;
  }
}
