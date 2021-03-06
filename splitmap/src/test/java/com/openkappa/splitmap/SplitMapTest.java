package com.openkappa.splitmap;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SplitMapTest {

  @Test
  public void testWriteToSplitMap() {
    SplitMapPageWriter writer = new SplitMapPageWriter();
    writer.add(1);
    writer.add(11);
    writer.add(1 << 16 | 1);
    SplitMap splitMap = writer.toSplitMap();
    assertTrue(splitMap.contains(1));
    assertTrue(splitMap.contains(11));
    assertTrue(splitMap.contains(1 << 16 | 1));
  }


  @Test
  public void testWriteToSplitMapWithPermutationHash() {
    SplitMapPageWriter writer = new SplitMapPageWriter(Involutions::reverse);
    writer.add(1);
    writer.add(11);
    writer.add(1 << 16 | 1);
    SplitMap splitMap = writer.toSplitMap();
    assertTrue(splitMap.contains(1));
    assertTrue(splitMap.contains(11));
    assertTrue(splitMap.contains(1 << 16 | 1));
  }

  @Test
  public void testCardinality() {
    SplitMapPageWriter writer = new SplitMapPageWriter();
    writer.add(1);
    writer.add(1 << 14);
    writer.add(1 << 16 | 1);
    writer.add(1 << 17);
    assertEquals(writer.toSplitMap().getCardinality(), 4);
  }

}