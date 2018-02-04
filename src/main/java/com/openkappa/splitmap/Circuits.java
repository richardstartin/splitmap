package com.openkappa.splitmap;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static java.util.stream.Collectors.toList;

public class Circuits {

  public static SplitMap evaluate(Function<List<Region>, Region> circuit,
                                  SplitMap... splitMaps) {
    return groupByKey(splitMaps)
            .streamUniformPartitions()
            .parallel()
            .collect(new IndexAggregator(circuit));
  }

  public static PrefixIndex<List<Region>> groupByKey(SplitMap... splitMaps) {
    PrefixIndex<List<Region>> grouped = new PrefixIndex<>();
    List<PrefixIndex<Region>> indices = Arrays.stream(splitMaps)
                                              .map(SplitMap::getIndex)
                                              .collect(toList());
    Region[] column = new Region[Long.SIZE];
    for (int i = 0; i < 1 << 10; ++i) {
      long word = 0L;
      for (PrefixIndex<Region> index : indices) {
        word = index.contributeToKey(i, word);
      }
      if (word != 0) {
        List<Region>[] chunk = new List[Long.SIZE];
        int limit = Long.bitCount(Long.highestOneBit(word) - 1) + 1;
        for (PrefixIndex<Region> index : indices) {
          index.readChunk(i, column);
          for (int j = 0; j < limit; ++j) {
            if (null != column[j]) {
              if (null == chunk[j]) {
                chunk[j] = new ArrayList<>();
              }
              chunk[j].add(column[j]);
            }
          }
        }
        for (int j = 0; j < limit; ++j) {
          grouped.insert((short)(i * Long.SIZE + j), chunk[j]);
        }
      }
    }
    return grouped;
  }

  private static class IndexAggregator implements Collector<PrefixIndex<List<Region>>, PrefixIndex<Region>, SplitMap> {

    private final Function<List<Region>, Region> circuit;
    private final ThreadLocal<Region[]> accumulatorChunkOut = ThreadLocal.withInitial(() -> new Region[Long.SIZE]);
    private final ThreadLocal<List<Region>[]> accumulatorChunkIn = ThreadLocal.withInitial(() -> new List[Long.SIZE]);

    // no two threads will ever write to the same partition because of the spliterator on the PrefixIndex
    private final PrefixIndex<Region> target = new PrefixIndex<>();

    public IndexAggregator(Function<List<Region>, Region> circuit) {
      this.circuit = circuit;
    }

    @Override
    public Supplier<PrefixIndex<Region>> supplier() {
      return () -> target;
    }

    @Override
    public BiConsumer<PrefixIndex<Region>, PrefixIndex<List<Region>>> accumulator() {
      return (l, r) -> {
        List<Region>[] chunkIn = accumulatorChunkIn.get();
        Region[] chunkOut = accumulatorChunkOut.get();
        for (int i = r.getMinChunkIndex(); i < r.getMaxChunkIndex(); ++i) {
          final long mask = r.contributeToKey(i, 0L);
          if (mask != 0 && r.readChunk(i, chunkIn)) {
            long temp = mask;
            while (temp != 0) {
              int j = numberOfTrailingZeros(temp);
              chunkOut[j] = circuit.apply(chunkIn[j]);
              temp ^= lowestOneBit(temp);
            }
            l.writeChunk(i, mask, chunkOut);
          }
        }
        l.project(r.getMinChunkIndex(), r.getMaxChunkIndex());
      };
    }

    @Override
    public BinaryOperator<PrefixIndex<Region>> combiner() {
      return (l, r) -> l;
    }

    @Override
    public Function<PrefixIndex<Region>, SplitMap> finisher() {
      return SplitMap::new;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return EnumSet.noneOf(Characteristics.class);
    }
  }
}
