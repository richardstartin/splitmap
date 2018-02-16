/*
 * Code taken and modified from RoaringBitmap project with the copyright notice below:
 *
 * (c) the authors Licensed under the Apache License, Version 2.0.
 * (https://github.com/RoaringBitmap/RoaringBitmap/blob/master/AUTHORS)
 */

package com.openkappa.splitmap.roaring;

/**
 * Iterator over short values.
 */
public interface MaskIterator extends Cloneable {
  /**
   * Creates a copy of the iterator.
   *
   * @return a clone of the current iterator
   */
  MaskIterator clone();

  /**
   * @return whether there is another value
   */
  boolean hasNext();

  /**
   * @return next short value as int value (using the least significant 16 bits)
   */
  int nextAsInt();

  short next();

  short peekNext();

  /**
   * If needed, advance as long as the next value is smaller than minval (as an unsigned
   * short)
   *
   * @param minval threshold
   */
  void advanceIfNeeded(short minval);

}
