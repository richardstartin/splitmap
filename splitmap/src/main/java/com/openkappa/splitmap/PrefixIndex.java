package com.openkappa.splitmap;

import java.util.function.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;

public class PrefixIndex<T> {

  static final int PARTITIONS;

  static {
    PARTITIONS = Runtime.getRuntime().availableProcessors();
  }
  static final int PARTITION_SIZE = (1 << 10) / PARTITIONS;

  private final long[] keys;
  private final ChunkedArray<T> values;
  private final int offset;
  private final int range;

  public PrefixIndex() {
    this(0, 1 << 10);
  }

  public PrefixIndex(long[] keys) {
    this(keys, new ChunkedArray<>(), 0, 1 << 10);
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
    if ((keys[pos >>> 6] & (1L << pos)) != 0) {
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
            .mapToObj(i -> new PrefixIndex<>(keys, values, PARTITION_SIZE * i, PARTITION_SIZE));
  }

  public void forEach(KeyValueConsumer<T> consumer) {
    int prefix = offset * Long.SIZE;
    for (int i = offset; i < offset + range; ++i) {
      long mask = keys[i];
      if (mask != 0) {
        T[] chunk = values.getChunkNoCopy(i);
        if (null != chunk) {
          while (mask != 0) {
            int j = numberOfTrailingZeros(mask);
            consumer.accept((short) (prefix + j), chunk[j]);
            mask ^= lowestOneBit(mask);
          }
        }
      }
      prefix += Long.SIZE;
    }
  }

  public long reduceLong(long initial, ToLongFunction<T> map, LongBinaryOperator reduce) {
    long result = initial;
    for (int i = offset; i < offset + range; ++i) {
      long mask = keys[i];
      if (mask != 0) {
        T[] chunk = values.getChunkNoCopy(i);
        if (null != chunk) {
          while (mask != 0) {
            result = reduce.applyAsLong(result, map.applyAsLong(chunk[numberOfTrailingZeros(mask)]));
            mask ^= lowestOneBit(mask);
          }
        }
      }
    }
    return result;
  }

  public double reduceDouble(double initial, ToDoubleFunction<T> map, DoubleBinaryOperator reduce) {
    double result = initial;
    for (int i = offset; i < offset + range; ++i) {
      long mask = keys[i];
      if (mask != 0) {
        T[] chunk = values.getChunkNoCopy(i);
        if (null != chunk) {
          while (mask != 0) {
            result = reduce.applyAsDouble(result, map.applyAsDouble(chunk[numberOfTrailingZeros(mask)]));
            mask ^= lowestOneBit(mask);
          }
        }
      }
    }
    return result;
  }

  public int reduceInt(int initial, ToIntFunction<T> map, IntBinaryOperator reduce) {
    int result = initial;
    for (int i = offset; i < offset + range; ++i) {
      long mask = keys[i];
      if (mask != 0) {
        T[] chunk = values.getChunkNoCopy(i);
        if (null != chunk) {
          while (mask != 0) {
            result = reduce.applyAsInt(result, map.applyAsInt(chunk[numberOfTrailingZeros(mask)]));
            mask ^= lowestOneBit(mask);
          }
        }
      }
    }
    return result;
  }

  public void writeChunk(int wordIndex, long word, T[] chunk) {
    keys[wordIndex] = word;
    if (word != 0 && null != chunk) {
      values.writeChunk(wordIndex, chunk);
    }
  }

  public void transferChunk(int wordIndex, long word, T[] chunk) {
    keys[wordIndex] = word;
    if (word != 0 && null != chunk) {
      values.transferChunk(wordIndex, chunk);
    }
  }

  public long computeKeyWord(int wordIndex, long value, LongBinaryOperator op) {
    return op.applyAsLong(keys[wordIndex], value);
  }

  public long readKeyWord(int wordIndex) {
    return keys[wordIndex];
  }

  public boolean readChunk(int chunkIndex, T[] ouptut) {
    return values.readChunk(chunkIndex, ouptut);
  }

  T[] getChunkNoCopy(int chunkIndex) {
    return values.getChunkNoCopy(chunkIndex);
  }

}
