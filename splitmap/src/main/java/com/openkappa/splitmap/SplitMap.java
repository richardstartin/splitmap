package com.openkappa.splitmap;

import com.openkappa.splitmap.roaring.Mask;

import java.util.function.IntUnaryOperator;
import java.util.stream.Stream;

public class SplitMap {

  private final PrefixIndex<Mask> index;
  private final IntUnaryOperator hash;

  public SplitMap(PrefixIndex<Mask> index, IntUnaryOperator hash) {
    this.index = index;
    this.hash = hash;
  }

  public SplitMap(PrefixIndex<Mask> index) {
    this(index, InvertibleHashing::scatter);
  }

  public SplitMap(IntUnaryOperator hash) {
    this(new PrefixIndex<>(), hash);
  }

  public void insert(short key, Mask region) {
    index.insert(key, region);
  }

  public boolean contains(int value) {
    Mask mask = index.get((short) hash.applyAsInt(value >>> 16));
    return null != mask && mask.contains((short) value);
  }

  public long getCardinality() {
    return index.reduceLong(0L, Mask::getCardinality, (x, y) -> x + y);
  }

  public boolean isEmpty() {
    return index.isEmpty();
  }

  public Stream<PrefixIndex<Mask>> stream() {
    return index.streamUniformPartitions();
  }

  PrefixIndex<Mask> getIndex() {
    return index;
  }

}
