package com.openkappa.splitmap;

public interface Involutions {

  static int reverse(int value) {
    return Integer.reverse(value) >>> 16;
  }

}
