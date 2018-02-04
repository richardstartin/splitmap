package com.openkappa.splitmap;

import java.util.Objects;

public class ChunkedArray<T> {

  private T[][] chunks = (T[][])new Object[1 << 10][];

  public T get(int index) {
    Objects.checkIndex(index >>> 6, chunks.length);
    T[] line = chunks[index >>> 6];
    return null == line ? null : line[index & 63];
  }

  public void put(int index, T value) {
    Objects.checkIndex(index >>> 6, chunks.length);
    T[] line = chunks[index >>> 6];
    if (null == line) {
      line = chunks[index >>> 6] = (T[])new Object[64];
    }
    line[index & 63] = value;
  }


}
