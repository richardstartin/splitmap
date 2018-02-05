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


  @Test
  public void testStringConcatenation() {
    PrefixIndex<String> index1 = new PrefixIndex<>();
    index1.insert((short)1, "foo");
    index1.insert((short)(3), "bar");
    PrefixIndex<String> index2 = new PrefixIndex<>();
    index2.insert((short)1, "bar");
    index2.insert((short)(2), "foo");

    PrefixIndex<String> concatenated = Circuits.groupByKey("", index1, index2)
            .streamUniformPartitions()
            .collect(new IndexAggregator<>(strings -> String.join( "|", strings)));

    assertEquals(concatenated.get((short)1), "foo|bar");
    assertEquals(concatenated.get((short)2), "|foo");
    assertEquals(concatenated.get((short)3), "bar|");
  }


  @Test
  public void testBoxedIntegerArithmetic() {
    PrefixIndex<Integer> index1 = new PrefixIndex<>();
    index1.insert((short)1, 10);
    index1.insert((short)(3), 11);
    PrefixIndex<Integer> index2 = new PrefixIndex<>();
    index2.insert((short)1, 9);
    index2.insert((short)(2), 12);

    PrefixIndex<Integer> summation = Circuits.groupByKey(0, index1, index2)
            .streamUniformPartitions()
            .collect(new IndexAggregator<>(numbers -> numbers.stream().mapToInt(i -> i).sum()));
    assertEquals((int)summation.get((short)1), 10 + 9);
    assertEquals((int)summation.get((short)2), 12);
    assertEquals((int)summation.get((short)3), 11);

  }

}