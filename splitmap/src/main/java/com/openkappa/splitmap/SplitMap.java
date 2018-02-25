package com.openkappa.splitmap;

import com.openkappa.splitmap.roaring.Mask;

import java.util.function.IntUnaryOperator;
import java.util.stream.Stream;

public class SplitMap {

  private final PrefixIndex<Mask> index;
  private final KeyInvolution involution;

  public SplitMap(PrefixIndex<Mask> index, KeyInvolution involution) {
    this.index = index;
    this.involution = involution;
  }

  public SplitMap(PrefixIndex<Mask> index) {
    this(index, Involutions::reverse);
  }

  public SplitMap(KeyInvolution involution) {
    this(new PrefixIndex<>(), involution);
  }

  public void insert(short key, Mask region) {
    index.insert(key, region);
  }

  public boolean contains(int value) {
    Mask mask = index.get(involution.invert((short)(value >>> 16)));
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
