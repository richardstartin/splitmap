package uk.co.openkappa.splitmap;

import org.roaringbitmap.Container;

import java.util.stream.Stream;

public class SplitMap {

  private final PrefixIndex<Container> index;
  private final KeyInvolution involution;

  public SplitMap(PrefixIndex<Container> index, KeyInvolution involution) {
    this.index = index;
    this.involution = involution;
  }

  public SplitMap(PrefixIndex<Container> index) {
    this(index, Involutions::reverse);
  }

  public SplitMap(KeyInvolution involution) {
    this(new PrefixIndex<>(), involution);
  }

  public void insert(short key, Container region) {
    index.insert(key, region);
  }

  public boolean contains(int value) {
    Container mask = index.get(involution.invert((short)(value >>> 16)));
    return null != mask && mask.contains((short) value);
  }

  public long getCardinality() {
    return index.reduceLong(0L, Container::getCardinality, (x, y) -> x + y);
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
