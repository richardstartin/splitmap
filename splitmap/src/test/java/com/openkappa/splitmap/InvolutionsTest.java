package com.openkappa.splitmap;

import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class InvolutionsTest {

  @Test
  public void testReverseIsNotSurjective() {
    Set<Short> outputs = new HashSet<>();
    for (int i = 0; i < 1 << 16; ++i) {
      assertTrue(outputs.add(Involutions.reverse((short)i)));
    }
  }


  @Test
  public void testReverseIsSelfInvertible() {
    for (int i = 0; i < 1 << 16; ++i) {
      short inverse = Involutions.reverse((short)i);
      assertEquals(Involutions.reverse(inverse) & 0xFFFF, i);
    }
  }

}