package com.openkappa.splitmap;

public interface InvertibleHashing {

  static int scatter(int value) {
    return Integer.reverse(value) >>> 16;
  }

}
