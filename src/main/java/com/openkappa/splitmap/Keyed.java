package com.openkappa.splitmap;

public class Keyed<T> implements Comparable<Keyed<T>> {
  private final short key;
  private final T value;

  public Keyed(short key, T value) {
    this.key = key;
    this.value = value;
  }

  public short getKey() {
    return key;
  }

  public T getValue() {
    return value;
  }

  @Override
  public int compareTo(Keyed<T> o) {
    return Short.compareUnsigned(key, o.key);
  }
}
