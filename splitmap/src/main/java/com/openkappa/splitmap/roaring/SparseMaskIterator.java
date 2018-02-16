package com.openkappa.splitmap.roaring;

final class SparseMaskIterator implements MaskIterator {
  int pos;
  SparseMask parent;

  SparseMaskIterator(SparseMask p) {
    wrap(p);
  }

  @Override
  public void advanceIfNeeded(short minval) {
    pos = Util.advanceUntil(parent.content, pos - 1, parent.cardinality, minval);
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
    return pos < parent.cardinality;
  }

  @Override
  public int nextAsInt() {
    return next() & 0xFFFF;
  }

  @Override
  public short next() {
    return parent.content[pos++];
  }

  void wrap(SparseMask p) {
    parent = p;
    pos = 0;
  }


  @Override
  public short peekNext() {
    return parent.content[pos];
  }

}
