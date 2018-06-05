package uk.co.openkappa.splitmap;

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

  @Test
  public void reduceLongShouldProduceCorrectResult() {
    PrefixIndex<String> index = new PrefixIndex<>();
    index.insert((short)0, "three blind mice");
    index.insert((short)129, "a little mouse with clogs on");
    long wordCount = index.reduceLong(0L, s -> s.split(" ").length, (x, y) -> x + y);
    assertEquals(wordCount, 9);
  }

  @Test
  public void reduceIntShouldProduceCorrectResult() {
    PrefixIndex<String> index = new PrefixIndex<>();
    index.insert((short)0, "three blind mice");
    index.insert((short)129, "a little mouse with clogs on");
    int wordCount = index.reduceInt(0, s -> s.split(" ").length, (x, y) -> x + y);
    assertEquals(wordCount, 9);
  }

}