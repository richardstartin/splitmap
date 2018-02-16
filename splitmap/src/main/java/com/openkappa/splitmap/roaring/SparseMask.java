/*
 * Code taken and modified from RoaringBitmap project with the copyright notice below:
 *
 * (c) the authors Licensed under the Apache License, Version 2.0.
 * (https://github.com/RoaringBitmap/RoaringBitmap/blob/master/AUTHORS)
 */

package com.openkappa.splitmap.roaring;

import java.util.Arrays;


/**
 * Simple mask made of an array of 16-bit integers
 */
public final class SparseMask extends Mask implements Cloneable {
  private static final int DEFAULT_INIT_SIZE = 4;
  private static final int ARRAY_LAZY_LOWERBOUND = 1024;

  static final int DEFAULT_MAX_SIZE = 4096;// masks with DEFAULT_MAX_SZE or less integers
  // should be ArrayMasks

  protected static int serializedSizeInBytes(int cardinality) {
    return cardinality * 2 + 2;
  }

  protected int cardinality = 0;

  short[] content;


  /**
   * Create an array mask with default capacity
   */
  public SparseMask() {
    this(DEFAULT_INIT_SIZE);
  }

  public static SparseMask empty() {
    return new SparseMask();
  }
  /**
   * Create an array mask with specified capacity
   *
   * @param capacity The capacity of the mask
   */
  public SparseMask(final int capacity) {
    content = new short[capacity];
  }

  /**
   * Create an array mask with a run of ones from firstOfRun to lastOfRun, inclusive. Caller is
   * responsible for making sure the range is small enough that SparseMask is appropriate.
   *
   * @param firstOfRun first index
   * @param lastOfRun last index (range is exclusive)
   */
  public SparseMask(final int firstOfRun, final int lastOfRun) {
    final int valuesInRange = lastOfRun - firstOfRun;
    this.content = new short[valuesInRange];
    for (int i = 0; i < valuesInRange; ++i) {
      content[i] = (short) (firstOfRun + i);
    }
    cardinality = valuesInRange;
  }

  /**
   * Create a new mask, no copy is made
   *
   * @param newCard desired cardinality
   * @param newContent actual values (length should equal or exceed cardinality)
   */
  public SparseMask(int newCard, short[] newContent) {
    this.cardinality = newCard;
    this.content = Arrays.copyOf(newContent, newCard);
  }

  protected SparseMask(short[] newContent) {
    this.cardinality = newContent.length;
    this.content = newContent;
  }

  private int advance(MaskIterator it) {
    if (it.hasNext()) {
      return it.nextAsInt();
    } else {
      return -1;
    }
  }

  @Override
  public SparseMask and(final SparseMask value2) {
    SparseMask value1 = this;
    final int desiredCapacity = Math.min(value1.getCardinality(), value2.getCardinality());
    SparseMask answer = new SparseMask(desiredCapacity);
    answer.cardinality = Util.unsignedIntersect2by2(value1.content, value1.getCardinality(),
            value2.content, value2.getCardinality(), answer.content);
    return answer;
  }

  @Override
  public Mask and(DenseMask x) {
    return x.and(this);
  }

  @Override
  // see andNot for an approach that might be better.
  public Mask and(RunMask x) {
    return x.and(this);
  }

  @Override
  public SparseMask andNot(final SparseMask value2) {
    SparseMask value1 = this;
    final int desiredCapacity = value1.getCardinality();
    SparseMask answer = new SparseMask(desiredCapacity);
    answer.cardinality = Util.unsignedDifference(value1.content, value1.getCardinality(),
            value2.content, value2.getCardinality(), answer.content);
    return answer;
  }

  @Override
  public SparseMask andNot(DenseMask value2) {
    final SparseMask answer = new SparseMask(content.length);
    int pos = 0;
    for (int k = 0; k < cardinality; ++k) {
      short val = this.content[k];
      answer.content[pos] = val;
      pos += 1 - value2.bitValue(val);
    }
    answer.cardinality = pos;
    return answer;
  }

  @Override
  public SparseMask andNot(RunMask x) {
    if (x.numberOfRuns() == 0) {
      return clone();
    } else if (x.isFull()) {
      return SparseMask.empty();
    }
    int write = 0;
    int read = 0;
    SparseMask answer = new SparseMask(cardinality);
    for (int i = 0; i < x.numberOfRuns() && read < cardinality; ++i) {
      int runStart = Util.toIntUnsigned(x.getValue(i));
      int runEnd = runStart + Util.toIntUnsigned(x.getLength(i));
      if (Util.toIntUnsigned(content[read]) > runEnd) {
        continue;
      }
      int firstInRun = Util.iterateUntil(content, read, cardinality, runStart);
      int toWrite = firstInRun - read;
      System.arraycopy(content, read, answer.content, write, toWrite);
      write += toWrite;

      read = Util.iterateUntil(content, firstInRun, cardinality, runEnd + 1);
    }
    System.arraycopy(content, read, answer.content, write, cardinality - read);
    write += cardinality - read;
    answer.cardinality = write;
    return answer;
  }

  @Override
  public SparseMask clone() {
    return new SparseMask(this.cardinality, this.content);
  }

  @Override
  public boolean contains(final short x) {
    return Util.unsignedBinarySearch(content, 0, cardinality, x) >= 0;
  }


  @Override
  protected boolean contains(RunMask runMask) {
    if (runMask.getCardinality() > cardinality) {
      return false;
    }
    int startPos, stopPos = -1;
    for (int i = 0; i < runMask.numberOfRuns(); ++i) {
      short start = runMask.getValue(i);
      int stop = Util.toIntUnsigned(start) + Util.toIntUnsigned(runMask.getLength(i));
      startPos = Util.advanceUntil(content, stopPos, cardinality, start);
      stopPos = Util.advanceUntil(content, stopPos, cardinality, (short)stop);
      if(startPos == cardinality) {
        return false;
      } else if(stopPos - startPos != stop - start
              || content[startPos] != start
              || content[stopPos] != stop) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean contains(SparseMask sparseMask) {
    if (cardinality < sparseMask.cardinality) {
      return false;
    }
    int i1 = 0, i2 = 0;
    while(i1 < cardinality && i2 < sparseMask.cardinality) {
      if(content[i1] == sparseMask.content[i2]) {
        ++i1;
        ++i2;
      } else if(Util.compareUnsigned(content[i1], sparseMask.content[i2]) < 0) {
        ++i1;
      } else {
        return false;
      }
    }
    return i2 == sparseMask.cardinality;
  }

  @Override
  protected boolean contains(DenseMask denseMask) {
    return false;
  }

  // in order
  private void emit(short val) {
    if (cardinality == content.length) {
      increaseCapacity(true);
    }
    content[cardinality++] = val;
  }


  @Override
  public boolean equals(Object o) {
    if (o instanceof SparseMask) {
      SparseMask srb = (SparseMask) o;
      if (srb.cardinality != this.cardinality) {
        return false;
      }
      for (int i = 0; i < this.cardinality; ++i) {
        if (this.content[i] != srb.content[i]) {
          return false;
        }
      }
      return true;
    } else if (o instanceof RunMask) {
      return o.equals(this);
    }
    return false;
  }

  @Override
  public int getCardinality() {
    return cardinality;
  }

  @Override
  public MaskIterator iterator() {
    return new SparseMaskIterator(this);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (int k = 0; k < cardinality; ++k) {
      hash += 31 * hash + content[k];
    }
    return hash;
  }

  private void increaseCapacity() {
    increaseCapacity(false);
  }


  // temporarily allow an illegally large size, as long as the operation creating
  // the illegal mask does not return it.
  private void increaseCapacity(boolean allowIllegalSize) {
    int newCapacity = (this.content.length == 0) ? DEFAULT_INIT_SIZE
            : this.content.length < 64 ? this.content.length * 2
            : this.content.length < 1067 ? this.content.length * 3 / 2
            : this.content.length * 5 / 4;
    // never allocate more than we will ever need
    if (newCapacity > SparseMask.DEFAULT_MAX_SIZE && !allowIllegalSize) {
      newCapacity = SparseMask.DEFAULT_MAX_SIZE;
    }
    // if we are within 1/16th of the max, go to max
    if (newCapacity > SparseMask.DEFAULT_MAX_SIZE - SparseMask.DEFAULT_MAX_SIZE / 16
            && !allowIllegalSize) {
      newCapacity = SparseMask.DEFAULT_MAX_SIZE;
    }
    this.content = Arrays.copyOf(this.content, newCapacity);
  }

  private void increaseCapacity(int min) {
    int newCapacity = (this.content.length == 0) ? DEFAULT_INIT_SIZE
            : this.content.length < 64 ? this.content.length * 2
            : this.content.length < 1024 ? this.content.length * 3 / 2
            : this.content.length * 5 / 4;
    if (newCapacity < min) {
      newCapacity = min;
    }
    // never allocate more than we will ever need
    if (newCapacity > SparseMask.DEFAULT_MAX_SIZE) {
      newCapacity = SparseMask.DEFAULT_MAX_SIZE;
    }
    // if we are within 1/16th of the max, go to max
    if (newCapacity > SparseMask.DEFAULT_MAX_SIZE - SparseMask.DEFAULT_MAX_SIZE / 16) {
      newCapacity = SparseMask.DEFAULT_MAX_SIZE;
    }
    this.content = Arrays.copyOf(this.content, newCapacity);
  }

  @Override
  public boolean intersects(SparseMask value2) {
    SparseMask value1 = this;
    return Util.unsignedIntersects(value1.content, value1.getCardinality(), value2.content,
            value2.getCardinality());
  }


  @Override
  public boolean intersects(DenseMask x) {
    return x.intersects(this);
  }

  @Override
  public boolean intersects(RunMask x) {
    return x.intersects(this);
  }


  @Override
  public Mask limit(int maxcardinality) {
    if (maxcardinality < this.getCardinality()) {
      return new SparseMask(maxcardinality, this.content);
    } else {
      return clone();
    }
  }

  protected void loadData(final DenseMask denseMask) {
    this.cardinality = denseMask.cardinality;
    denseMask.fillArray(content);
  }

  // for use in inot range known to be nonempty
  private void negateRange(final short[] buffer, final int startIndex, final int lastIndex,
                           final int startRange, final int lastRange) {
    // compute the negation into buffer

    int outPos = 0;
    int inPos = startIndex; // value here always >= valInRange,
    // until it is exhausted
    // n.b., we can start initially exhausted.

    int valInRange = startRange;
    for (; valInRange < lastRange && inPos <= lastIndex; ++valInRange) {
      if ((short) valInRange != content[inPos]) {
        buffer[outPos++] = (short) valInRange;
      } else {
        ++inPos;
      }
    }

    // if there are extra items (greater than the biggest
    // pre-existing one in range), buffer them
    for (; valInRange < lastRange; ++valInRange) {
      buffer[outPos++] = (short) valInRange;
    }

    if (outPos != buffer.length) {
      throw new RuntimeException(
              "negateRange: outPos " + outPos + " whereas buffer.length=" + buffer.length);
    }
    // copy back from buffer...caller must ensure there is room
    int i = startIndex;
    for (short item : buffer) {
      content[i++] = item;
    }
  }

  // shares lots of code with inot; candidate for refactoring
  @Override
  public Mask not(final int firstOfRange, final int lastOfRange) {
    // TODO: may need to convert to a RunMask
    if (firstOfRange >= lastOfRange) {
      return clone(); // empty range
    }

    // determine the span of array indices to be affected
    int startIndex = Util.unsignedBinarySearch(content, 0, cardinality, (short) firstOfRange);
    if (startIndex < 0) {
      startIndex = -startIndex - 1;
    }
    int lastIndex = Util.unsignedBinarySearch(content, 0, cardinality, (short) (lastOfRange - 1));
    if (lastIndex < 0) {
      lastIndex = -lastIndex - 2;
    }
    final int currentValuesInRange = lastIndex - startIndex + 1;
    final int spanToBeFlipped = lastOfRange - firstOfRange;
    final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
    final int cardinalityChange = newValuesInRange - currentValuesInRange;
    final int newCardinality = cardinality + cardinalityChange;

    if (newCardinality > DEFAULT_MAX_SIZE) {
      return toBitmapMask().not(firstOfRange, lastOfRange);
    }

    SparseMask answer = new SparseMask(newCardinality);

    // copy stuff before the active area
    System.arraycopy(content, 0, answer.content, 0, startIndex);

    int outPos = startIndex;
    int inPos = startIndex; // item at inPos always >= valInRange

    int valInRange = firstOfRange;
    for (; valInRange < lastOfRange && inPos <= lastIndex; ++valInRange) {
      if ((short) valInRange != content[inPos]) {
        answer.content[outPos++] = (short) valInRange;
      } else {
        ++inPos;
      }
    }

    for (; valInRange < lastOfRange; ++valInRange) {
      answer.content[outPos++] = (short) valInRange;
    }

    // content after the active range
    for (int i = lastIndex + 1; i < cardinality; ++i) {
      answer.content[outPos++] = content[i];
    }
    answer.cardinality = newCardinality;
    return answer;
  }

  @Override
  int numberOfRuns() {
    if (cardinality == 0) {
      return 0; // should never happen
    }
    int numRuns = 1;
    int oldv = Util.toIntUnsigned(content[0]);
    for (int i = 1; i < cardinality; i++) {
      int newv = Util.toIntUnsigned(content[i]);
      if (oldv + 1 != newv) {
        ++numRuns;
      }
      oldv = newv;
    }
    return numRuns;
  }

  @Override
  public Mask or(final SparseMask value2) {
    final SparseMask value1 = this;
    int totalCardinality = value1.getCardinality() + value2.getCardinality();
    if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
      DenseMask bc = new DenseMask();
      for (int k = 0; k < value2.cardinality; ++k) {
        short v = value2.content[k];
        final int i = Util.toIntUnsigned(v) >>> 6;
        bc.bitmap[i] |= (1L << v);
      }
      for (int k = 0; k < this.cardinality; ++k) {
        short v = this.content[k];
        final int i = Util.toIntUnsigned(v) >>> 6;
        bc.bitmap[i] |= (1L << v);
      }
      bc.cardinality = 0;
      for (long k : bc.bitmap) {
        bc.cardinality += Long.bitCount(k);
      }
      if (bc.cardinality <= DEFAULT_MAX_SIZE) {
        return bc.toArrayMask();
      } else if (bc.isFull(cardinality)) {
        return RunMask.full();
      }
      return bc;
    }
    SparseMask answer = new SparseMask(totalCardinality);
    answer.cardinality =
            Util.unsignedUnion2by2(
                    value1.content, 0, value1.getCardinality(),
                    value2.content, 0, value2.getCardinality(),
                    answer.content
            );
    return answer;
  }

  @Override
  public Mask or(DenseMask x) {
    return x.or(this);
  }

  @Override
  public Mask or(RunMask x) {
    return x.or(this);
  }

  protected Mask or(MaskIterator it) {
    return or(it, false);
  }

  /**
   * it must return items in (unsigned) sorted order. Possible candidate for Mask interface?
   **/
  private Mask or(MaskIterator it, final boolean exclusive) {
    SparseMask ac = new SparseMask();
    int myItPos = 0;
    ac.cardinality = 0;
    // do a merge. int -1 denotes end of input.
    int myHead = (myItPos == cardinality) ? -1 : Util.toIntUnsigned(content[myItPos++]);
    int hisHead = advance(it);

    while (myHead != -1 && hisHead != -1) {
      if (myHead < hisHead) {
        ac.emit((short) myHead);
        myHead = (myItPos == cardinality) ? -1 : Util.toIntUnsigned(content[myItPos++]);
      } else if (myHead > hisHead) {
        ac.emit((short) hisHead);
        hisHead = advance(it);
      } else {
        if (!exclusive) {
          ac.emit((short) hisHead);
        }
        hisHead = advance(it);
        myHead = (myItPos == cardinality) ? -1 : Util.toIntUnsigned(content[myItPos++]);
      }
    }

    while (myHead != -1) {
      ac.emit((short) myHead);
      myHead = (myItPos == cardinality) ? -1 : Util.toIntUnsigned(content[myItPos++]);
    }

    while (hisHead != -1) {
      ac.emit((short) hisHead);
      hisHead = advance(it);
    }

    if (ac.cardinality > DEFAULT_MAX_SIZE) {
      return ac.toBitmapMask();
    } else {
      return ac;
    }
  }

  @Override
  public Mask repairAfterLazy() {
    return this;
  }

  private short select(int j) {
    return this.content[j];
  }

  /**
   * Copies the data in a bitmap container.
   *
   * @return the bitmap mask
   */
  @Override
  public DenseMask toBitmapMask() {
    DenseMask bc = new DenseMask();
    bc.loadData(this);
    return bc;
  }

  @Override
  public int first() {
    assertNonEmpty(cardinality == 0);
    return Util.toIntUnsigned(content[0]);
  }

  @Override
  public int last() {
    assertNonEmpty(cardinality == 0);
    return Util.toIntUnsigned(content[cardinality - 1]);
  }

  @Override
  public String toString() {
    if (this.cardinality == 0) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (int i = 0; i < this.cardinality - 1; i++) {
      sb.append(this.content[i]);
      sb.append(",");
    }
    sb.append(this.content[this.cardinality - 1]);
    sb.append("}");
    return sb.toString();
  }

  @Override
  public Mask xor(final SparseMask value2) {
    final SparseMask value1 = this;
    final int totalCardinality = value1.getCardinality() + value2.getCardinality();
    if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
      DenseMask bc = new DenseMask();
      for (int k = 0; k < value2.cardinality; ++k) {
        short v = value2.content[k];
        final int i = Util.toIntUnsigned(v) >>> 6;
        bc.bitmap[i] ^= (1L << v);
      }
      for (int k = 0; k < this.cardinality; ++k) {
        short v = this.content[k];
        final int i = Util.toIntUnsigned(v) >>> 6;
        bc.bitmap[i] ^= (1L << v);
      }
      bc.cardinality = 0;
      for (long k : bc.bitmap) {
        bc.cardinality += Long.bitCount(k);
      }
      if (bc.cardinality <= DEFAULT_MAX_SIZE) {
        return bc.toArrayMask();
      }
      return bc;
    }
    SparseMask answer = new SparseMask(totalCardinality);
    answer.cardinality = Util.unsignedExclusiveUnion2by2(value1.content, value1.getCardinality(),
            value2.content, value2.getCardinality(), answer.content);
    return answer;
  }

  @Override
  public Mask xor(DenseMask x) {
    return x.xor(this);
  }

  @Override
  public Mask xor(RunMask x) {
    return x.xor(this);
  }


  protected Mask xor(MaskIterator it) {
    return or(it, true);
  }

  protected Mask lazyor(SparseMask value2) {
    final SparseMask value1 = this;
    int totalCardinality = value1.getCardinality() + value2.getCardinality();
    if (totalCardinality > ARRAY_LAZY_LOWERBOUND) {// it could be a bitmap!
      DenseMask bc = new DenseMask();
      for (int k = 0; k < value2.cardinality; ++k) {
        short v = value2.content[k];
        final int i = Util.toIntUnsigned(v) >>> 6;
        bc.bitmap[i] |= (1L << v);
      }
      for (int k = 0; k < this.cardinality; ++k) {
        short v = this.content[k];
        final int i = Util.toIntUnsigned(v) >>> 6;
        bc.bitmap[i] |= (1L << v);
      }
      bc.cardinality = -1;
      return bc;
    }
    SparseMask answer = new SparseMask(totalCardinality);
    answer.cardinality =
            Util.unsignedUnion2by2(
                    value1.content, 0, value1.getCardinality(),
                    value2.content, 0, value2.getCardinality(),
                    answer.content
            );
    return answer;

  }

}


