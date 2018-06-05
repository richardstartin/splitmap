package uk.co.openkappa.splitmap;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class ChunkedDoubleArrayTest {

  @Test
  public void getPageNoCopyShouldGiveBackTransferredPageBack() {
    double[] page = new double[1 << 10];
    ChunkedDoubleArray array = new ChunkedDoubleArray();
    array.transfer(1, page);
    double[] result = array.getPageNoCopy(1);
    assertTrue(page == result);
  }


  @Test
  public void writtenPageShouldBeCopied() {
    double[] page = new double[1 << 10];
    ChunkedDoubleArray array = new ChunkedDoubleArray();
    array.write(1, page);
    double[] result = array.getPageNoCopy(1);
    assertFalse(page == result);
  }


  @Test
  public void getPageNoCopyShouldGiveInsertedPageBack() {
    double[] page = new double[1 << 10];
    page[10] = Math.PI;
    ChunkedDoubleArray array = new ChunkedDoubleArray();
    array.write(63, page);
    double[] buffer = new double[1 << 10];
    assertTrue(array.writeTo(63, buffer));
    assertEquals(buffer, page);
  }

  @Test
  public void writeMissingPageToBufferShouldNotSucceed() {
    double[] page = new double[1 << 10];
    page[10] = Math.PI;
    ChunkedDoubleArray array = new ChunkedDoubleArray();
    array.write(63, page);
    double[] buffer = new double[1 << 10];
    assertFalse(array.writeTo(62, buffer));
  }


  @Test
  public void reduceShouldProduceCorrectValue() {
    double[] page = new double[1 << 10];
    page[10] = Math.PI;
    page[100] = 20.9;
    ChunkedDoubleArray array = new ChunkedDoubleArray();
    array.write(1, page);
    page[10] = 0;
    page[100] = 0;
    page[1000] = 19.5;
    array.write(62, page);
    double result = array.reduce(100, Reduction::add);
    assertEquals(result, Math.PI + 20.9 + 19.5 + 100, 1E-9);
  }

}