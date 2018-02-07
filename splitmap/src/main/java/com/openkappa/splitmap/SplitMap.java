package com.openkappa.splitmap;

import org.roaringbitmap.Container;

import java.util.function.IntUnaryOperator;

public class SplitMap {

  private final PrefixIndex<Container> index;
  private final IntUnaryOperator hash;

  public SplitMap(PrefixIndex<Container> index, IntUnaryOperator hash) {
    this.index = index;
    this.hash = hash;
  }

  public SplitMap(PrefixIndex<Container> index) {
    this (index, IntUnaryOperator.identity());
  }

  public SplitMap(IntUnaryOperator hash) {
    this(new PrefixIndex<>(), hash);
  }

  public SplitMap() {
    this(new PrefixIndex<>(), IntUnaryOperator.identity());
  }

  public void insert(short key, Container region) {
    index.insert(key, region);
  }

  public boolean contains(int value) {
    Container container = index.get((short) hash.applyAsInt(value >>> 16));
    return null != container && container.contains((short) value);
  }

  public long getCardinality() {
    return index.streamUniformPartitions()
      .mapToInt(partition -> partition.reduceInt(0, Container::getCardinality, (x, y) -> x + y))
      .sum();
  }

  PrefixIndex<Container> getIndex() {
    return index;
  }

}
