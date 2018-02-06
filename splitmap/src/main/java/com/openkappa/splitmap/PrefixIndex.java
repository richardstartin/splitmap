package com.openkappa.splitmap;

import java.util.function.Function;
import java.util.function.LongBinaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class PrefixIndex<T> {

  private static final int PARTITIONS;
  private static final int PARTITION_SIZE = (1 << 10) / PARTITIONS;

  static {
    PARTITIONS = Runtime.getRuntime().availableProcessors();
  }

  private final long[] keys;
  private final ChunkedArray<T> values;
  private final int offset;
  private final int range;

  public PrefixIndex() {
    this(0, 1 << 10);
  }

  PrefixIndex(long[] keys, ChunkedArray<T> values, int offset, int range) {
    this.keys = keys;
    this.values = values;
    this.offset = offset;
    this.range = range;
  }

  PrefixIndex(int offset, int range) {
    this(new long[1 << 10], new ChunkedArray<>(), offset, range);
  }

  public int getMinChunkIndex() {
    return offset;
  }

  public int getMaxChunkIndex() {
    return offset + range;
  }

  public T get(short key) {
    int pos = key & 0xFFFF;
    if ((keys[pos >>> 6] & (1 << pos)) != 0) {
      return values.get(pos);
    }
    return null;
  }

  public void insert(short key, T value) {
    int pos = key & 0xFFFF;
    keys[pos >>> 6] |= (1L << pos);
    values.put(pos, value);
  }

  public Stream<PrefixIndex<T>> streamUniformPartitions() {
    return IntStream.range(0, PARTITIONS)
            .mapToObj(i -> new PrefixIndex<>(keys, values, PARTITION_SIZE * i, PARTITIONS));
  }

  public Stream<T> stream() {
    return IntStream.range(offset, offset + range)
            .filter(i -> keys[i] != 0)
            .mapToObj(values::streamChunk)
            .flatMap(Function.identity());
  }

  public void writeChunk(int wordIndex, long word, T[] chunk) {
    keys[wordIndex] = word;
    if (word != 0) {
      values.writeChunk(wordIndex, chunk);
    }
  }

  public long contributeToKey(int wordIndex, long value, LongBinaryOperator op) {
    return op.applyAsLong(keys[wordIndex], value);
  }

  public boolean readChunk(int chunkIndex, T[] ouptut) {
    return values.readChunk(chunkIndex, ouptut);
  }

}
