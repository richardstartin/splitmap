package com.openkappa.splitmap;

import java.util.Arrays;

public class DirtyRegion implements Region {

  private final long[] words;
  private int cardinality = -1;

  public DirtyRegion(long[] words, int cardinality) {
    this.words = words;
    this.cardinality = cardinality;
  }

  public DirtyRegion(long[] words) {
    this(words, -1);
  }

  public DirtyRegion() {
    this(new long[1 << 10], -1);
  }


  @Override
  public boolean isEmpty() {
    if (cardinality == -1) {
      for (int i = 0; i < words.length; ++i) {
        if (words[i] != 0) {
          return false;
        }
      }
      return true;
    }
    return cardinality == 0;
  }

  @Override
  public boolean contains(short value) {
    int pos = value & 0xFFFF;
    return (words[pos >>> 6] & (1L << pos)) != 0;
  }

  @Override
  public Region xor(Region other) {
    if (other instanceof DirtyRegion) {
      DirtyRegion dr = (DirtyRegion)other;
      long[] result = Arrays.copyOf(words, words.length);
     for (int i = 0; i < result.length; ++i) {
       result[i] ^= dr.words[i];
     }
     return new DirtyRegion(result);
    }
    throw new RuntimeException("not implemented");
  }

  @Override
  public int cardinality() {
    if (-1 == cardinality) {
      cardinality = computeCardinality();
    }
    return cardinality;
  }

  private int computeCardinality() {
    int cardinality = 0;
    for (int i = 0; i < words.length; ++i) {
      cardinality += Long.bitCount(words[i]);
    }
    return cardinality;
  }
}
