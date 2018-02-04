package com.openkappa.splitmap;

public class PrefixIndex<T> {

  private final long[] keys = new long[1 << 10];
  private final ChunkedArray<T> values = new ChunkedArray<>();


  public T get(short key) {
    int pos = key & 0xFFFF;
    if ((keys[pos >>> 6] & (1 << pos)) != 0) {
      return values.get(pos);
    }
    return null;
  }

  public void insert(short key, T value) {
    int pos = key & 0xFFFF;
    keys[pos >>> 6] |= (1L << pos);
    values.put(pos, value);
  }


}
