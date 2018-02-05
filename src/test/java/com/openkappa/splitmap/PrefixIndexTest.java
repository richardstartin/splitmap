package com.openkappa.splitmap;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class PrefixIndexTest {

  @Test
  public void ifValueDoesntExisteGetNull() {
    PrefixIndex<String> index = new PrefixIndex<>();
    assertNull(index.get((short) 0));
  }

  @Test
  public void shouldRetrieveInsertedValue() {
    PrefixIndex<String> index = new PrefixIndex<>();
    index.insert((short) 1, "value 1");
    index.insert((short) 65, "value 2");
    assertEquals("value 1", index.get((short) 1));
    assertEquals("value 2", index.get((short) 65));
  }

}