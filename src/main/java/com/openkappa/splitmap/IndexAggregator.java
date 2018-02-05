package com.openkappa.splitmap;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;
import static java.util.stream.Collector.Characteristics.UNORDERED;

class IndexAggregator<T> implements Collector<PrefixIndex<List<T>>, PrefixIndex<T>, PrefixIndex<T>> {

  private final Function<List<T>, T> circuit;
  private final ThreadLocal<T[]> bufferOut = ThreadLocal.withInitial(() -> (T[])new Object[Long.SIZE]);
  private final ThreadLocal<List<T>[]> bufferIn = ThreadLocal.withInitial(() -> new List[Long.SIZE]);

  // no two threads will ever write to the same partition because of the spliterator on the PrefixIndex
  private final PrefixIndex<T> target = new PrefixIndex<>();

  public IndexAggregator(Function<List<T>, T> circuit) {
    this.circuit = circuit;
  }

  @Override
  public Supplier<PrefixIndex<T>> supplier() {
    return () -> target;
  }

  @Override
  public BiConsumer<PrefixIndex<T>, PrefixIndex<List<T>>> accumulator() {
    return (l, r) -> {
      List<T>[] chunkIn = bufferIn.get();
      T[] chunkOut = bufferOut.get();
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
  public BinaryOperator<PrefixIndex<T>> combiner() {
    return (l, r) -> l;
  }

  @Override
  public Function<PrefixIndex<T>, PrefixIndex<T>> finisher() {
    return Function.identity();
  }

  @Override
  public Set<Characteristics> characteristics() {
    return EnumSet.of(UNORDERED, IDENTITY_FINISH);
  }
}
