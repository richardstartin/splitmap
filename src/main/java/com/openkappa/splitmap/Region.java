package com.openkappa.splitmap;

public interface Region {
  boolean isEmpty();

  boolean contains(short value);

  Region xor(Region other);

  int cardinality();
}
