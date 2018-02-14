package com.openkappa.splitmap;

import org.roaringbitmap.Container;
import org.roaringbitmap.RunContainer;
import org.roaringbitmap.ShortIterator;

public class ContainerUtils {

  private static final Container[] RANGES = new Container[1 << 8];
  static {
    for (int i = 0; i < 1 << 8; ++i) {
      RANGES[i] = new RunContainer().add(i * 256, (i + 1) * 256);
    }
  }

  public static boolean contains256BitRange(Container container, int min) {
    assert min % 256 == 0;
    return container.contains(RANGES[min >>> 8]);
  }
}
