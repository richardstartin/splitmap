package uk.co.openkappa.splitmap;

import org.testng.annotations.Test;

import static java.util.Map.entry;
import static java.util.Map.ofEntries;
import static org.testng.Assert.assertTrue;

public class CircuitsTest {

  private static QueryContext<Integer, ?> contextOf(SplitMapPageWriter one, SplitMapPageWriter two) {
    return new QueryContext<>(ofEntries(entry(0, one.toSplitMap()), entry(1, two.toSplitMap())), null);
  }

  @Test
  public void testIntersect() {
    SplitMapPageWriter w1 = new SplitMapPageWriter();
    w1.add(0);
    w1.add(1);
    w1.add(1 << 16);
    w1.add(1 << 16 | 1);
    w1.add(1 << 17 | 1);

    SplitMapPageWriter w2 = new SplitMapPageWriter();
    w2.add(0);
    w2.add(2);
    w2.add(1 << 16);
    w2.add(1 << 16 | 2);
    w2.add(1 << 17 | 2);

    SplitMap result = Circuits.evaluateIfKeysIntersect(contextOf(w1, w2),
            slice -> null == slice ? null : slice.get(0).and(slice.get(1)),
            0, 1);

    assertTrue(result.contains(0));
    assertTrue(result.contains(1 << 16));
  }

  @Test
  public void testCircuit() {
    SplitMapPageWriter w1 = new SplitMapPageWriter();
    w1.add(0);
    w1.add(1);
    w1.add(1 << 16);
    w1.add(1 << 16 | 1);
    w1.add(1 << 17 | 1);

    SplitMapPageWriter w2 = new SplitMapPageWriter();
    w2.add(0);
    w2.add(2);
    w2.add(1 << 16);
    w2.add(1 << 16 | 2);
    w2.add(1 << 17 | 2);

    SplitMap result = Circuits.evaluate(contextOf(w1, w2), slice -> null == slice ? null : slice.get(0).xor(slice.get(1)), 0, 1);

    assertTrue(result.contains(1));
    assertTrue(result.contains(2));
    assertTrue(result.contains(1 << 16 | 1));
    assertTrue(result.contains(1 << 16 | 2));
    assertTrue(result.contains(1 << 17 | 1));
    assertTrue(result.contains(1 << 17 | 2));
  }

}