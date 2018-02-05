package com.openkappa.splitmap;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class CircuitsTest {

  @Test
  public void testIntersect() {
    PageWriter w1 = new PageWriter();
    w1.add(0);
    w1.add(1);
    w1.add(1 << 16);
    w1.add(1 << 16 | 1);
    w1.add(1 << 17 | 1);

    PageWriter w2 = new PageWriter();
    w2.add(0);
    w2.add(2);
    w2.add(1 << 16);
    w2.add(1 << 16 | 2);
    w2.add(1 << 17 | 2);

    SplitMap result = Circuits.evaluateIfKeysIntersect(
            slice -> null == slice ? null : slice.get(0).and(slice.get(1)),
            w1.toSplitMap(), w2.toSplitMap());

    assertTrue(result.contains(0));
    assertTrue(result.contains(1 << 16));
  }

  @Test
  public void testCircuit() {
    PageWriter w1 = new PageWriter();
    w1.add(0);
    w1.add(1);
    w1.add(1 << 16);
    w1.add(1 << 16 | 1);
    w1.add(1 << 17 | 1);

    PageWriter w2 = new PageWriter();
    w2.add(0);
    w2.add(2);
    w2.add(1 << 16);
    w2.add(1 << 16 | 2);
    w2.add(1 << 17 | 2);

    SplitMap result = Circuits.evaluate(slice -> null == slice ? null : slice.get(0).xor(slice.get(1)),
            w1.toSplitMap(), w2.toSplitMap());

    assertTrue(result.contains(1));
    assertTrue(result.contains(2));
    assertTrue(result.contains(1 << 16 | 1));
    assertTrue(result.contains(1 << 16 | 2));
    assertTrue(result.contains(1 << 17 | 1));
    assertTrue(result.contains(1 << 17 | 2));
  }

}