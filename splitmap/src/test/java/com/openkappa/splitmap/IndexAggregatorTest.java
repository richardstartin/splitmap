package com.openkappa.splitmap;

import org.roaringbitmap.Container;
import org.roaringbitmap.RunContainer;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static org.testng.Assert.*;

public class IndexAggregatorTest {


  private static QueryContext<Integer, ?> ctx(long... keyWords) {
    Map<Integer, SplitMap> filters = new HashMap<>();
    for (int i = 0; i < keyWords.length; ++i) {
      long[] keys = new long[1 << 10];
      PrefixIndex<Container> index = new PrefixIndex<>(keys);
      long mask = keyWords[i];
      while (mask != 0) {
        index.insert((short) numberOfTrailingZeros(mask), RunContainer.full());
        mask ^= lowestOneBit(mask);
      }
      filters.put(i, new SplitMap(index));
    }
    return new QueryContext<>(filters, null);
  }

  @Test
  public void keysEvaluatingToNullShouldBeDroppedFromResult() {
    SplitMap sm = Circuits.evaluate(ctx(1L << 2, 1L << 2, 1L << 62), slice -> null, 0, 1);
    assertTrue(sm.isEmpty());
    assertEquals(0L, sm.getCardinality());
  }

  @Test
  public void keysEvaluatingToEmptyAreNotTreatedSpecially() {
    SplitMap sm = Circuits.evaluate(ctx(1L << 2, 1L << 2),
            slice -> slice.get(0).andNot(slice.get(1)), 0, 1);
    assertFalse(sm.isEmpty());
    assertEquals(0L, sm.getCardinality());
  }
}