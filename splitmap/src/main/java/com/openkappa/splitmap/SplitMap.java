package com.openkappa.splitmap;

import org.roaringbitmap.Container;

public class SplitMap {

  private final PrefixIndex<Container> index;

  public SplitMap(PrefixIndex<Container> index) {
    this.index = index;
  }

  public SplitMap() {
    this(new PrefixIndex<>());
  }

  public void insert(short key, Container region) {
    index.insert(key, region);
  }

  public boolean contains(int value) {
    Container container = index.get((short) (value >>> 16));
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
