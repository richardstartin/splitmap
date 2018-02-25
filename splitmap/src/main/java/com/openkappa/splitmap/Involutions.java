package com.openkappa.splitmap;

public interface Involutions {

  static short reverse(short value) {
    return (short)(Integer.reverse(value) >>> 16);
  }

  static short reversePreserveLS64(short value) {
    return (short)((Integer.reverse(value & 0xFFC0) >>> 16) | (value & 0x3F));
  }

}
