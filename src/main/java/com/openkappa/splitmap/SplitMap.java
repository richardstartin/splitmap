package com.openkappa.splitmap;

public class SplitMap {

  private final PrefixIndex<Region> index;

  public SplitMap(PrefixIndex<Region> index) {
    this.index = index;
  }

  public SplitMap() {
    this(new PrefixIndex<>());
  }

  public void insert(short key, Region region) {
    index.insert(key, region);
  }

  public boolean contains(int value) {
    return index.get((short)(value >>> 16)).contains((short)value);
  }
}
