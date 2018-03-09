package com.openkappa.splitmap;

public interface Involutions {

  static short reverse(short value) {
    return (short)(Integer.reverse(value) >>> 16);
  }

}
