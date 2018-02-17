package com.openkappa.splitmap;

import com.openkappa.splitmap.roaring.Mask;
import com.openkappa.splitmap.roaring.RunMask;

public class MaskUtils {

  private static final Mask[] RANGES_1024 = new Mask[1 << 6];
  static {
    for (int i = 0; i < 1 << 6; ++i) {
      RANGES_1024[i] = new RunMask(i * 1024, (i + 1) * 1024);
    }
  }



  public static boolean contains1024BitRange(Mask mask, int min) {
    assert min % 1024 == 0;
    return mask.contains(RANGES_1024[min >>> 10]);
  }
}
