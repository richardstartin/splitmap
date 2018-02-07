package com.openkappa.splitmap;

import java.util.concurrent.ThreadLocalRandom;

public class Hashing {

  public static int permute(int value) {
    return SCATTER[value & 0xFFFF];
  }

  private static short[] SCATTER = new short[1 << 16];
  static {
    long[] bits = new long[1 << 10];
    int count = 0;
    int index = 0;
    while (count < (1 << 16)) {
      int value = ThreadLocalRandom.current().nextInt(1 << 16);
      long bit = (1L << value);
      if ((bits[value >>> 6] & bit) == 0) {
        SCATTER[index++] = (short)value;
        ++count;
        bits[value >>> 6] |= bit;
      }
    }
  }

}
