package com.openkappa.splitmap;

public class InvertibleHashing {

  public static int scatter(int value) {
    return Integer.reverse(value) >>> 16;
  }

}
