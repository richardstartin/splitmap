package com.openkappa.splitmap.roaring;

final class DenseMaskIterator implements MaskIterator {

  long w;
  int x;

  long[] bitmap;

  DenseMaskIterator(long[] p) {
    wrap(p);
  }

  @Override
  public MaskIterator clone() {
    try {
      return (MaskIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;// will not happen
    }
  }

  @Override
  public boolean hasNext() {
    return x < bitmap.length;
  }


  @Override
  public int nextAsInt() {
    return next() & 0xFFFF;
  }

  @Override
  public short next() {
    long t = w & -w;
    short answer = (short)(x * 64 + Long.bitCount(t - 1));
    w ^= t;
    while (w == 0) {
      ++x;
      if (x == bitmap.length) {
        break;
      }
      w = bitmap[x];
    }
    return answer;
  }

  public void wrap(long[] b) {
    bitmap = b;
    for (x = 0; x < bitmap.length; ++x) {
      if ((w = bitmap[x]) != 0) {
        break;
      }
    }
  }

  @Override
  public void advanceIfNeeded(short minval) {
    if (Util.toIntUnsigned(minval) >= (x + 1) * 64) {
      x = Util.toIntUnsigned(minval) / 64;
      w = bitmap[x];
      while (w == 0) {
        ++x;
        if (x == bitmap.length) {
          return;
        }
        w = bitmap[x];
      }
    }
    while (hasNext() && (Util.toIntUnsigned(peekNext()) < Util.toIntUnsigned(minval))) {
      nextAsInt(); // could be optimized
    }
  }

  @Override
  public short peekNext() {
    long t = w & -w;
    return (short) (x * 64 + Long.bitCount(t - 1));
  }
}
