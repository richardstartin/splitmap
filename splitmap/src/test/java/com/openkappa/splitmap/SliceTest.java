package com.openkappa.splitmap;

import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SliceTest {

  @Test
  public void shouldGetPopulatedValueBack() {
    Slice<String, String> slice = new Slice<>("default");
    slice.set("foo", "bar");
    assertEquals("bar", slice.get("foo"));
  }

  @Test
  public void shouldGetDefaultValueIfUnset() {
    Slice<String, String> slice = new Slice<>("default");
    assertEquals("default", slice.get("foo"));
  }


  @Test
  public void shouldGetPopulatedValuesWhenIterating() {
    Slice<String, String> slice = new Slice<>("default");
    slice.set("foo", "bar");
    slice.set("bar", "foo");
    List<String> all = StreamSupport.stream(slice.spliterator(), false).collect(Collectors.toList());
    assertEquals(2, all.size());
    assertTrue(all.contains("foo"));
    assertTrue(all.contains("bar"));
  }

}