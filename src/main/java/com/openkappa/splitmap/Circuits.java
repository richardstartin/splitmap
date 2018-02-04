package com.openkappa.splitmap;

import org.roaringbitmap.Container;

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

  public static SplitMap evaluate(Function<List<Container>, Container> circuit,
                                  SplitMap... splitMaps) {
    return groupByKey(splitMaps)
            .streamUniformPartitions()
            .parallel()
            .collect(new IndexAggregator(circuit));
  }

  public static PrefixIndex<List<Container>> groupByKey(SplitMap... splitMaps) {
    PrefixIndex<List<Container>> grouped = new PrefixIndex<>();
    List<PrefixIndex<Container>> indices = Arrays.stream(splitMaps)
                                              .map(SplitMap::getIndex)
                                              .collect(toList());
    Container[] column = new Container[Long.SIZE];
    for (int i = 0; i < 1 << 10; ++i) {
      long word = 0L;
      for (PrefixIndex<Container> index : indices) {
        word = index.contributeToKey(i, word);
      }
      if (word != 0) {
        List<Container>[] chunk = new List[Long.SIZE];
        int limit = Long.bitCount(Long.highestOneBit(word) - 1) + 1;
        for (PrefixIndex<Container> index : indices) {
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

  private static class IndexAggregator implements Collector<PrefixIndex<List<Container>>, PrefixIndex<Container>, SplitMap> {

    private final Function<List<Container>, Container> circuit;
    private final ThreadLocal<Container[]> accumulatorChunkOut = ThreadLocal.withInitial(() -> new Container[Long.SIZE]);
    private final ThreadLocal<List<Container>[]> accumulatorChunkIn = ThreadLocal.withInitial(() -> new List[Long.SIZE]);

    // no two threads will ever write to the same partition because of the spliterator on the PrefixIndex
    private final PrefixIndex<Container> target = new PrefixIndex<>();

    public IndexAggregator(Function<List<Container>, Container> circuit) {
      this.circuit = circuit;
    }

    @Override
    public Supplier<PrefixIndex<Container>> supplier() {
      return () -> target;
    }

    @Override
    public BiConsumer<PrefixIndex<Container>, PrefixIndex<List<Container>>> accumulator() {
      return (l, r) -> {
        List<Container>[] chunkIn = accumulatorChunkIn.get();
        Container[] chunkOut = accumulatorChunkOut.get();
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
    public BinaryOperator<PrefixIndex<Container>> combiner() {
      return (l, r) -> l;
    }

    @Override
    public Function<PrefixIndex<Container>, SplitMap> finisher() {
      return SplitMap::new;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return EnumSet.noneOf(Characteristics.class);
    }
  }
}
