package com.openkappa.splitmap;

import com.openkappa.splitmap.roaring.Mask;
import com.openkappa.splitmap.roaring.SparseMask;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.LongBinaryOperator;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;

public class Circuits {

  private static final Mask EMPTY = new SparseMask();

  public static <Filter>
  SplitMap evaluateIfKeysIntersect(QueryContext<Filter, ?> context,
                                   Function<Slice<Filter, Mask>, Mask> circuit,
                                   Filter... filters) {
    return new SplitMap(groupByIntersectingKeys(context, EMPTY, filters)
            .streamUniformPartitions()
            .parallel()
            .collect(new IndexAggregator<>(circuit)));
  }


  public static <Filter>
  SplitMap evaluate(QueryContext<Filter, ?> context,
                    Function<Slice<Filter, Mask>, Mask> circuit,
                    Filter... filters) {
    return new SplitMap(groupByKey(context, EMPTY, filters)
            .streamUniformPartitions()
            .parallel()
            .collect(new IndexAggregator<>(circuit)));
  }

  static <T, Filter>
  PrefixIndex<Slice<Filter, T>> groupByKey(QueryContext<Filter, ?> context,
                                           T defaultValue, Filter... filters) {
    return groupByKey(context, (x, y) -> x | y, 0L, defaultValue, filters);
  }

  static <T, Filter>
  PrefixIndex<Slice<Filter, T>> groupByIntersectingKeys(QueryContext<Filter, ?> context,
                                                        T defaultValue,
                                                        Filter... filters) {
    return groupByKey(context, (x, y) -> x & y, -1L, defaultValue, filters);
  }

  private static <T, Filter>
  PrefixIndex<Slice<Filter, T>> groupByKey(QueryContext<Filter, ?> context,
                                           LongBinaryOperator op,
                                           long identity,
                                           T defaultValue,
                                           Filter... filters) {
    PrefixIndex<Slice<Filter, T>> grouped = new PrefixIndex<>();
    PrefixIndex<T>[] indices = Arrays.stream(filters)
            .map(filter -> context.getSplitMap(filter).getIndex()).toArray(PrefixIndex[]::new);

    for (int wordIndex = 0; wordIndex < 1024; ++wordIndex) {
      long word = identity;
      for (PrefixIndex<T> index : indices) {
        word = index.computeKeyWord(wordIndex, word, op);
      }
      if (word != 0) {
        Slice<Filter, T>[] chunk = new Slice[Long.SIZE];
        int k = 0;
        for (PrefixIndex<T> index : indices) {
          T[] column = index.getChunkNoCopy(wordIndex);
          if (null == column) {
            continue;
          }
          long mask = word;
          while (mask != 0) {
            int j = numberOfTrailingZeros(mask);
            if (null == chunk[j]) {
              chunk[j] = new Slice<>(defaultValue);
            }
            chunk[j].set(filters[k], column[j]);
            mask ^= lowestOneBit(mask);
          }
          ++k;
        }
        grouped.transferChunk(wordIndex, word, chunk);
      }
    }
    return grouped;
  }
}
