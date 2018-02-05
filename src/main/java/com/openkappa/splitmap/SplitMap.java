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
    return index.get((short)(value >>> 16)).contains((short)value);
  }

  PrefixIndex<Container> getIndex() {
    return index;
  }
}
