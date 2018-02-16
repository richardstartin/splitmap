package com.openkappa.splitmap;

import com.openkappa.splitmap.roaring.Mask;
import com.openkappa.splitmap.roaring.RunMask;

public class MaskUtils {

  private static final Mask[] RANGES = new Mask[1 << 8];
  static {
    for (int i = 0; i < 1 << 8; ++i) {
      RANGES[i] = new RunMask(i * 256, (i + 1) * 256);
    }
  }

  public static boolean contains256BitRange(Mask mask, int min) {
    assert min % 256 == 0;
    return mask.contains(RANGES[min >>> 8]);
  }
}
