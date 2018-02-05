package com.openkappa.splitmap;

import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.Container;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static java.util.stream.Collectors.toList;

public class Circuits {

  private static final Container EMPTY = new ArrayContainer();

  public static SplitMap evaluateIfKeysIntersect(Function<List<Container>, Container> circuit, SplitMap... splitMaps) {
    return groupByKey((x, y) -> x & y, -1L, splitMaps)
            .streamUniformPartitions()
            .parallel()
            .collect(new IndexAggregator(circuit));
  }


  public static SplitMap evaluate(Function<List<Container>, Container> circuit, SplitMap... splitMaps) {
    return groupByKey((x, y) -> x | y, 0L, splitMaps)
            .streamUniformPartitions()
            .parallel()
            .collect(new IndexAggregator(circuit));
  }

  public static PrefixIndex<List<Container>> groupByKey(LongBinaryOperator op,
                                                        long identity,
                                                        SplitMap... splitMaps) {
    PrefixIndex<List<Container>> grouped = new PrefixIndex<>();
    List<PrefixIndex<Container>> indices = Arrays.stream(splitMaps).map(SplitMap::getIndex).collect(toList());
    List<Container> prototype = IntStream.range(0, splitMaps.length).mapToObj(i -> EMPTY).collect(toList());
    Container[] column = new Container[Long.SIZE];
    for (int i = 0; i < 1 << 10; ++i) {
      long word = identity;
      for (PrefixIndex<Container> index : indices) {
        word = index.contributeToKey(i, word, op);
      }
      if (word != 0) {
        List<Container>[] chunk = new List[Long.SIZE];
        int k = 0;
        for (PrefixIndex<Container> index : indices) {
          index.readChunk(i, column);
          long mask = word;
          while (mask != 0) {
            int j = numberOfTrailingZeros(mask);
            if (null != column[j]) {
              if (null == chunk[j]) {
                chunk[j] = new ArrayList<>(prototype);
                grouped.insert((short)(i * Long.SIZE + j), chunk[j]);
              }
              chunk[j].set(k, column[j]);
            }
            mask ^= lowestOneBit(mask);
          }
          ++k;
        }
      }
    }
    return grouped;
  }

  private static class IndexAggregator implements Collector<PrefixIndex<List<Container>>, PrefixIndex<Container>, SplitMap> {

    private final Function<List<Container>, Container> circuit;
    private final ThreadLocal<Container[]> bufferOut = ThreadLocal.withInitial(() -> new Container[Long.SIZE]);
    private final ThreadLocal<List<Container>[]> bufferIn = ThreadLocal.withInitial(() -> new List[Long.SIZE]);

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
        List<Container>[] chunkIn = bufferIn.get();
        Container[] chunkOut = bufferOut.get();
        for (int i = r.getMinChunkIndex(); i < r.getMaxChunkIndex(); ++i) {
          final long mask = r.contributeToKey(i, 0L, (x, y) -> x | y);
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
      return EnumSet.of(Characteristics.UNORDERED);
    }
  }
}
