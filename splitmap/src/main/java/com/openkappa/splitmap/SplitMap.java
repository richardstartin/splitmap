package com.openkappa.splitmap;

import org.roaringbitmap.Container;

import java.util.function.IntUnaryOperator;
import java.util.stream.Stream;

public class SplitMap {

  private final PrefixIndex<Container> index;
  private final IntUnaryOperator hash;

  public SplitMap(PrefixIndex<Container> index, IntUnaryOperator hash) {
    this.index = index;
    this.hash = hash;
  }

  public SplitMap(PrefixIndex<Container> index) {
    this(index, InvertibleHashing::scatter);
  }

  public SplitMap(IntUnaryOperator hash) {
    this(new PrefixIndex<>(), hash);
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
            .mapToLong(partition -> partition.reduceInt(0, Container::getCardinality, (x, y) -> x + y))
            .sum();
  }

  public boolean isEmpty() {
    return index.isEmpty();
  }

  public Stream<PrefixIndex<Container>> stream() {
    return index.streamUniformPartitions();
  }

  PrefixIndex<Container> getIndex() {
    return index;
  }

}
