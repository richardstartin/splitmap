package com.openkappa.splitmap;

import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.*;

public class HashingTest {

  @Test
  public void testScatterHashIsNotSurjective() {
    Set<Short> outputs = new HashSet<>();
    for (int i = 0; i < 1 << 16; ++i) {
      assertTrue(outputs.add((short)Hashing.scatter(i)));
    }
  }

}