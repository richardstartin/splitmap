package com.openkappa.splitmap.roaring;

import static com.openkappa.splitmap.roaring.Util.toIntUnsigned;

final class RunMaskIterator implements MaskIterator {
  int pos;
  int le = 0;
  int maxlength;
  int base;

  RunMask parent;

  RunMaskIterator(RunMask p) {
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
    return pos < parent.nbrruns;
  }

  @Override
  public short next() {
    short ans = (short) (base + le);
    le++;
    if (le > maxlength) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = toIntUnsigned(parent.getLength(pos));
        base = toIntUnsigned(parent.getValue(pos));
      }
    }
    return ans;
  }

  @Override
  public int nextAsInt() {
    return next() & 0xFFFF;
  }

  @Override
  public short peekNext() {
    return (short) (base + le);
  }

  void wrap(RunMask p) {
    parent = p;
    pos = 0;
    le = 0;
    if (pos < parent.nbrruns) {
      maxlength = toIntUnsigned(parent.getLength(pos));
      base = toIntUnsigned(parent.getValue(pos));
    }
  }

  @Override
  public void advanceIfNeeded(short minval) {
    while (base + maxlength < toIntUnsigned(minval)) {
      pos++;
      le = 0;
      if (pos < parent.nbrruns) {
        maxlength = toIntUnsigned(parent.getLength(pos));
        base = toIntUnsigned(parent.getValue(pos));
      } else {
        return;
      }
    }
    if (base > toIntUnsigned(minval)) {
      return;
    }
    le = toIntUnsigned(minval) - base;
  }

}
