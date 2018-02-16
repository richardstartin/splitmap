/*
 * Code taken and modified from RoaringBitmap project with the copyright notice below:
 *
 * (c) the authors Licensed under the Apache License, Version 2.0.
 * (https://github.com/RoaringBitmap/RoaringBitmap/blob/master/AUTHORS)
 */

package com.openkappa.splitmap.roaring;

import java.util.Arrays;


/**
 * This mask takes the form of runs of consecutive values (effectively, run-length encoding).
 *
 * Adding and removing content from this mask might make it wasteful so regular calls to
 * "runOptimize" might be warranted.
 */
public final class RunMask extends Mask implements Cloneable {
  private static final int DEFAULT_INIT_SIZE = 4;
  private static final boolean ENABLE_GALLOPING_AND = false;

  private static int branchyUnsignedInterleavedBinarySearch(final short[] array, final int begin,
                                                            final int end, final short k) {
    int ikey = Util.toIntUnsigned(k);
    int low = begin;
    int high = end - 1;
    while (low <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = Util.toIntUnsigned(array[2 * middleIndex]);
      if (middleValue < ikey) {
        low = middleIndex + 1;
      } else if (middleValue > ikey) {
        high = middleIndex - 1;
      } else {
        return middleIndex;
      }
    }
    return -(low + 1);
  }

  // starts with binary search and finishes with a sequential search
  private static int hybridUnsignedInterleavedBinarySearch(final short[] array, final int begin,
                                                           final int end, final short k) {
    int ikey = Util.toIntUnsigned(k);
    int low = begin;
    int high = end - 1;
    // 16 in the next line matches the size of a cache line
    while (low + 16 <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = Util.toIntUnsigned(array[2 * middleIndex]);
      if (middleValue < ikey) {
        low = middleIndex + 1;
      } else if (middleValue > ikey) {
        high = middleIndex - 1;
      } else {
        return middleIndex;
      }
    }
    // we finish the job with a sequential search
    int x = low;
    for (; x <= high; ++x) {
      final int val = Util.toIntUnsigned(array[2 * x]);
      if (val >= ikey) {
        if (val == ikey) {
          return x;
        }
        break;
      }
    }
    return -(x + 1);
  }

  protected static int serializedSizeInBytes(int numberOfRuns) {
    return 2 + 2 * 2 * numberOfRuns; // each run requires 2 2-byte entries.
  }

  private static int unsignedInterleavedBinarySearch(final short[] array, final int begin,
                                                     final int end, final short k) {
    if (Util.USE_HYBRID_BINSEARCH) {
      return hybridUnsignedInterleavedBinarySearch(array, begin, end, k);
    } else {
      return branchyUnsignedInterleavedBinarySearch(array, begin, end, k);
    }

  }

  private short[] valueslength;// we interleave values and lengths, so
  // that if you have the values 11,12,13,14,15, you store that as 11,4 where 4 means that beyond 11
  // itself, there are
  // 4 contiguous values that follows.
  // Other example: e.g., 1, 10, 20,0, 31,2 would be a concise representation of 1, 2, ..., 11, 20,
  // 31, 32, 33

  int nbrruns = 0;// how many runs, this number should fit in 16 bits.


  /**
   * Create a mask with default capacity
   */
  public RunMask() {
    this(DEFAULT_INIT_SIZE);
  }

  public RunMask(int from, int to) {
    assert from < to;
    this.valueslength = new short[]{(short)from, (short)(to - from)};
  }


  protected RunMask(SparseMask arr, int nbrRuns) {
    this.nbrruns = nbrRuns;
    valueslength = new short[2 * nbrRuns];
    if (nbrRuns == 0) {
      return;
    }

    int prevVal = -2;
    int runLen = 0;
    int runCount = 0;

    for (int i = 0; i < arr.cardinality; i++) {
      int curVal = Util.toIntUnsigned(arr.content[i]);
      if (curVal == prevVal + 1) {
        ++runLen;
      } else {
        if (runCount > 0) {
          setLength(runCount - 1, (short) runLen);
        }
        setValue(runCount, (short) curVal);
        runLen = 0;
        ++runCount;
      }
      prevVal = curVal;
    }
    setLength(runCount - 1, (short) runLen);
  }

  /**
   * Create an array mask with specified capacity
   *
   * @param capacity The capacity of the mask
   */
  public RunMask(final int capacity) {
    valueslength = new short[2 * capacity];
  }


  private RunMask(int nbrruns, short[] valueslength) {
    this.nbrruns = nbrruns;
    this.valueslength = Arrays.copyOf(valueslength, valueslength.length);
  }

  /**
   * Construct a new RunMask backed by the provided array. Note that if you modify the
   * RunMask a new array may be produced.
   *
   * @param array array where the data is stored
   * @param numRuns number of runs (each using 2 shorts in the buffer)
   *
   */
  public RunMask(final short[] array, final int numRuns) {
    if (array.length < 2 * numRuns) {
      throw new RuntimeException("Mismatch between buffer and numRuns");
    }
    this.nbrruns = numRuns;
    this.valueslength = array;
  }

  @Override
  public Mask and(SparseMask x) {
    SparseMask ac = new SparseMask(x.cardinality);
    if (this.nbrruns == 0) {
      return ac;
    }
    int rlepos = 0;
    int arraypos = 0;

    int rleval = Util.toIntUnsigned(this.getValue(rlepos));
    int rlelength = Util.toIntUnsigned(this.getLength(rlepos));
    while (arraypos < x.cardinality) {
      int arrayval = Util.toIntUnsigned(x.content[arraypos]);
      while (rleval + rlelength < arrayval) {// this will frequently be false
        ++rlepos;
        if (rlepos == this.nbrruns) {
          return ac;// we are done
        }
        rleval = Util.toIntUnsigned(this.getValue(rlepos));
        rlelength = Util.toIntUnsigned(this.getLength(rlepos));
      }
      if (rleval > arrayval) {
        arraypos = Util.advanceUntil(x.content, arraypos, x.cardinality, (short)rleval);
      } else {
        ac.content[ac.cardinality] = (short) arrayval;
        ac.cardinality++;
        arraypos++;
      }
    }
    return ac;
  }


  @Override
  public Mask and(DenseMask x) {
    // could be implemented as return toBitmapOrArrayMask().iand(x);
    int card = this.getCardinality();
    if (card <= SparseMask.DEFAULT_MAX_SIZE) {
      // result can only be an array (assuming that we never make a RunMask)
      if (card > x.cardinality) {
        card = x.cardinality;
      }
      SparseMask answer = new SparseMask(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int runStart = Util.toIntUnsigned(this.getValue(rlepos));
        int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));
        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          if (x.contains((short) runValue)) {// it looks like contains() should be cheap enough if
            // accessed sequentially
            answer.content[answer.cardinality++] = (short) runValue;
          }
        }
      }
      return answer;
    }
    // we expect the answer to be a bitmap (if we are lucky)
    DenseMask answer = x.clone();
    int start = 0;
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int end = Util.toIntUnsigned(this.getValue(rlepos));
      int prevOnes = answer.cardinalityInRange(start, end);
      Util.resetBitmapRange(answer.bitmap, start, end); // had been x.bitmap
      answer.updateCardinality(prevOnes, 0);
      start = end + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
    }
    int ones = answer.cardinalityInRange(start, DenseMask.MAX_CAPACITY);
    Util.resetBitmapRange(answer.bitmap, start, DenseMask.MAX_CAPACITY); // had been x.bitmap
    answer.updateCardinality(ones, 0);
    if (answer.getCardinality() > SparseMask.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayMask();
    }
  }

  @Override
  public Mask and(RunMask x) {
    RunMask answer = new RunMask(new short[2 * (this.nbrruns + x.nbrruns)], 0);
    int rlepos = 0;
    int xrlepos = 0;
    int start = Util.toIntUnsigned(this.getValue(rlepos));
    int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
    int xstart = Util.toIntUnsigned(x.getValue(xrlepos));
    int xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        if (ENABLE_GALLOPING_AND) {
          rlepos = skipAhead(this, rlepos, xstart); // skip over runs until we have end > xstart (or
          // rlepos is advanced beyond end)
        } else {
          ++rlepos;
        }

        if (rlepos < this.nbrruns) {
          start = Util.toIntUnsigned(this.getValue(rlepos));
          end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
      } else if (xend <= start) {
        // exit the second run
        if (ENABLE_GALLOPING_AND) {
          xrlepos = skipAhead(x, xrlepos, start);
        } else {
          ++xrlepos;
        }

        if (xrlepos < x.nbrruns) {
          xstart = Util.toIntUnsigned(x.getValue(xrlepos));
          xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
        }
      } else {// they overlap
        final int lateststart = start > xstart ? start : xstart;
        int earliestend;
        if (end == xend) {// improbable
          earliestend = end;
          rlepos++;
          xrlepos++;
          if (rlepos < this.nbrruns) {
            start = Util.toIntUnsigned(this.getValue(rlepos));
            end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
          }
          if (xrlepos < x.nbrruns) {
            xstart = Util.toIntUnsigned(x.getValue(xrlepos));
            xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
          }
        } else if (end < xend) {
          earliestend = end;
          rlepos++;
          if (rlepos < this.nbrruns) {
            start = Util.toIntUnsigned(this.getValue(rlepos));
            end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
          }

        } else {// end > xend
          earliestend = xend;
          xrlepos++;
          if (xrlepos < x.nbrruns) {
            xstart = Util.toIntUnsigned(x.getValue(xrlepos));
            xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
          }
        }
        answer.valueslength[2 * answer.nbrruns] = (short) lateststart;
        answer.valueslength[2 * answer.nbrruns + 1] = (short) (earliestend - lateststart - 1);
        answer.nbrruns++;
      }
    }
    return answer.toEfficientMask(); // subsequent trim() may be required to avoid wasted
    // space.
  }

  @Override
  public Mask andNot(SparseMask x) {
    // when x is small, we guess that the result will still be a run container
    final int arbitrary_threshold = 32; // this is arbitrary
    if (x.getCardinality() < arbitrary_threshold) {
      return lazyandNot(x).toEfficientMask();
    }
    // otherwise we generate either an array or bitmap mask
    final int card = getCardinality();
    if (card <= SparseMask.DEFAULT_MAX_SIZE) {
      // if the cardinality is small, we construct the solution in place
      SparseMask ac = new SparseMask(card);
      ac.cardinality =
              Util.unsignedDifference(this.iterator(), x.iterator(), ac.content);
      return ac;
    }
    // otherwise, we generate a bitmap
    return toBitmapOrArrayMask(card).andNot(x);
  }

  @Override
  public Mask andNot(DenseMask x) {
    // could be implemented as toTemporaryBitmap().iandNot(x);
    int card = this.getCardinality();
    if (card <= SparseMask.DEFAULT_MAX_SIZE) {
      // result can only be an array (assuming that we never make a RunMask)
      SparseMask answer = new SparseMask(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int runStart = Util.toIntUnsigned(this.getValue(rlepos));
        int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));
        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          if (!x.contains((short) runValue)) {// it looks like contains() should be cheap enough if
            // accessed sequentially
            answer.content[answer.cardinality++] = (short) runValue;
          }
        }
      }
      return answer;
    }
    // we expect the answer to be a bitmap (if we are lucky)
    DenseMask answer = x.clone();
    int lastPos = 0;
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(this.getValue(rlepos));
      int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
      int prevOnes = answer.cardinalityInRange(lastPos, start);
      int flippedOnes = answer.cardinalityInRange(start, end);
      Util.resetBitmapRange(answer.bitmap, lastPos, start);
      Util.flipBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnes + flippedOnes, end - start - flippedOnes);
      lastPos = end;
    }
    int ones = answer.cardinalityInRange(lastPos, DenseMask.MAX_CAPACITY);
    Util.resetBitmapRange(answer.bitmap, lastPos, DenseMask.MAX_CAPACITY);
    answer.updateCardinality(ones, 0);
    if (answer.getCardinality() > SparseMask.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayMask();
    }
  }

  @Override
  public Mask andNot(RunMask x) {
    RunMask answer = new RunMask(new short[2 * (this.nbrruns + x.nbrruns)], 0);
    int rlepos = 0;
    int xrlepos = 0;
    int start = Util.toIntUnsigned(this.getValue(rlepos));
    int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
    int xstart = Util.toIntUnsigned(x.getValue(xrlepos));
    int xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        // output the first run
        answer.valueslength[2 * answer.nbrruns] = (short) start;
        answer.valueslength[2 * answer.nbrruns + 1] = (short) (end - start - 1);
        answer.nbrruns++;
        rlepos++;
        if (rlepos < this.nbrruns) {
          start = Util.toIntUnsigned(this.getValue(rlepos));
          end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
      } else if (xend <= start) {
        // exit the second run
        xrlepos++;
        if (xrlepos < x.nbrruns) {
          xstart = Util.toIntUnsigned(x.getValue(xrlepos));
          xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
        }
      } else {
        if (start < xstart) {
          answer.valueslength[2 * answer.nbrruns] = (short) start;
          answer.valueslength[2 * answer.nbrruns + 1] = (short) (xstart - start - 1);
          answer.nbrruns++;
        }
        if (xend < end) {
          start = xend;
        } else {
          rlepos++;
          if (rlepos < this.nbrruns) {
            start = Util.toIntUnsigned(this.getValue(rlepos));
            end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
          }
        }
      }
    }
    if (rlepos < this.nbrruns) {
      answer.valueslength[2 * answer.nbrruns] = (short) start;
      answer.valueslength[2 * answer.nbrruns + 1] = (short) (end - start - 1);
      answer.nbrruns++;
      rlepos++;
      if (rlepos < this.nbrruns) {
        System.arraycopy(this.valueslength, 2 * rlepos, answer.valueslength, 2 * answer.nbrruns,
                2 * (this.nbrruns - rlepos));
        answer.nbrruns = answer.nbrruns + this.nbrruns - rlepos;
      }
    }
    return answer.toEfficientMask();
  }

  // Append a value length with all values until a given value
  private void appendValueLength(int value, int index) {
    int previousValue = Util.toIntUnsigned(getValue(index));
    int length = Util.toIntUnsigned(getLength(index));
    int offset = value - previousValue;
    if (offset > length) {
      setLength(index, (short) offset);
    }
  }

  // To check if a value length can be prepended with a given value
  private boolean canPrependValueLength(int value, int index) {
    if (index < this.nbrruns) {
      int nextValue = Util.toIntUnsigned(getValue(index));
      if (nextValue == value + 1) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Mask clone() {
    return new RunMask(nbrruns, valueslength);
  }

  @Override
  public boolean contains(short x) {
    int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
    if (index >= 0) {
      return true;
    }
    index = -index - 2; // points to preceding value, possibly -1
    if (index != -1) {// possible match
      int offset = Util.toIntUnsigned(x) - Util.toIntUnsigned(getValue(index));
      int le = Util.toIntUnsigned(getLength(index));
      if (offset <= le) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected boolean contains(RunMask runMask) {
    int i1 = 0, i2 = 0;
    while(i1 < numberOfRuns() && i2 < runMask.numberOfRuns()) {
      int start1 = Util.toIntUnsigned(getValue(i1));
      int stop1 = start1 + Util.toIntUnsigned(getLength(i1));
      int start2 = Util.toIntUnsigned(runMask.getValue(i2));
      int stop2 = start2 + Util.toIntUnsigned(runMask.getLength(i2));
      if(start1 > start2) {
        return false;
      } else {
        if(stop1 > stop2) {
          i2++;
        } else if(stop1 == stop2) {
          i1++;
          i2++;
        } else {
          i1++;
        }
      }
    }
    return i2 == runMask.numberOfRuns();
  }

  @Override
  protected boolean contains(SparseMask sparseMask) {
    final int cardinality = getCardinality();
    final int runCount = numberOfRuns();
    if (sparseMask.getCardinality() > cardinality) {
      return false;
    }
    int ia = 0, ir = 0;
    while(ia < sparseMask.getCardinality() && ir <= runCount) {
      int start = getValue(ir);
      int stop = start + Util.toIntUnsigned(getLength(ir));
      if(sparseMask.content[ia] < start) {
        return false;
      } else if (sparseMask.content[ia] > stop) {
        ++ir;
      } else {
        ++ia;
      }
    }
    return ia <= cardinality && ir <= runCount;
  }

  @Override
  protected boolean contains(DenseMask denseMask) {
    final int cardinality = getCardinality();
    if (denseMask.getCardinality() != -1 && denseMask.getCardinality() > cardinality) {
      return false;
    }
    final int runCount = numberOfRuns();
    short ib = 0, ir = 0;
    while(ib < denseMask.bitmap.length && ir < runCount) {
      long w = denseMask.bitmap[ib];
      while (w != 0 && ir < runCount) {
        short start = getValue(ir);
        int stop = start+ Util.toIntUnsigned(getLength(ir));
        long t = w & -w;
        long r = ib * 64 + Long.numberOfTrailingZeros(w);
        if (r < start) {
          return false;
        } else if(r > stop) {
          ++ir;
        } else {
          w ^= t;
        }
      }
      if(w == 0) {
        ++ib;
      } else {
        return false;
      }
    }
    if(ib < denseMask.bitmap.length) {
      for(; ib < denseMask.bitmap.length ; ib++) {
        if(denseMask.bitmap[ib] != 0) {
          return false;
        }
      }
    }
    return true;
  }


  // a very cheap check... if you have more than 4096, then you should use a bitmap mask.
  // this function avoids computing the cardinality
  private Mask convertToLazyBitmapIfNeeded() {
    // when nbrruns exceed SparseMask.DEFAULT_MAX_SIZE, then we know it should be stored as a
    // bitmap, always
    if (this.nbrruns > SparseMask.DEFAULT_MAX_SIZE) {
      DenseMask answer = new DenseMask();
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int start = Util.toIntUnsigned(this.getValue(rlepos));
        int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        Util.setBitmapRange(answer.bitmap, start, end);
      }
      answer.cardinality = -1;
      return answer;
    }
    return this;
  }



  // Push all values length to the end of the array (resize array if needed)
  private void copyToOffset(int offset) {
    final int minCapacity = 2 * (offset + nbrruns);
    if (valueslength.length < minCapacity) {
      // expensive case where we need to reallocate
      int newCapacity = valueslength.length;
      while (newCapacity < minCapacity) {
        newCapacity = (newCapacity == 0) ? DEFAULT_INIT_SIZE
                : newCapacity < 64 ? newCapacity * 2
                : newCapacity < 1024 ? newCapacity * 3 / 2 : newCapacity * 5 / 4;
      }
      short[] newvalueslength = new short[newCapacity];
      copyValuesLength(this.valueslength, 0, newvalueslength, offset, nbrruns);
      this.valueslength = newvalueslength;
    } else {
      // efficient case where we just copy
      copyValuesLength(this.valueslength, 0, this.valueslength, offset, nbrruns);
    }
  }

  private void copyValuesLength(short[] src, int srcIndex, short[] dst, int dstIndex, int length) {
    System.arraycopy(src, 2 * srcIndex, dst, 2 * dstIndex, 2 * length);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RunMask) {
      return equals((RunMask) o);
    } else if (o instanceof SparseMask) {
      return equals((SparseMask) o);
    } else if (o instanceof Mask) {
      if (((Mask) o).getCardinality() != this.getCardinality()) {
        return false; // should be a frequent branch if they differ
      }
      // next bit could be optimized if needed:
      MaskIterator me = this.iterator();
      MaskIterator you = ((Mask) o).iterator();
      while (me.hasNext()) {
        if (me.next() != you.next()) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private boolean equals(RunMask runMask) {
    if (runMask.nbrruns != this.nbrruns) {
      return false;
    }
    for (int i = 0; i < nbrruns; ++i) {
      if (getValue(i) != runMask.getValue(i)) {
        return false;
      }
      if (getLength(i) != runMask.getLength(i)) {
        return false;
      }
    }
    return true;
  }

  private boolean equals(SparseMask sparseMask) {
    int pos = 0;
    for (short i = 0; i < nbrruns; ++i) {
      short runStart = getValue(i);
      int length = Util.toIntUnsigned(getLength(i));
      if (pos + length >= sparseMask.getCardinality()) {
        return false;
      }
      if (sparseMask.content[pos] != runStart) {
        return false;
      }
      if (sparseMask.content[pos + length] != (short) (Util.toIntUnsigned(runStart) + length)) {
        return false;
      }
      pos += length + 1;
    }
    return pos == sparseMask.getCardinality();
  }

  @Override
  public int getCardinality() {
    int sum = nbrruns;// lengths are returned -1
    for (int k = 0; k < nbrruns; ++k) {
      sum = sum + Util.toIntUnsigned(getLength(k))/* + 1 */;
    }
    return sum;
  }

  public short getLength(int index) {
    return valueslength[2 * index + 1];
  }

  @Override
  public MaskIterator iterator() {
    return new RunMaskIterator(this);
  }

  public short getValue(int index) {
    return valueslength[2 * index];
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (int k = 0; k < nbrruns * 2; ++k) {
      hash += 31 * hash + valueslength[k];
    }
    return hash;
  }

  private Mask ilazyorToRun(SparseMask x) {
    if (isFull()) {
      return full();
    }
    final int nbrruns = this.nbrruns;
    final int offset = Math.max(nbrruns, x.getCardinality());
    copyToOffset(offset);
    int rlepos = 0;
    this.nbrruns = 0;
    MaskIterator i = x.iterator();
    while (i.hasNext() && (rlepos < nbrruns)) {
      if (Util.compareUnsigned(getValue(rlepos + offset), i.peekNext()) <= 0) {
        smartAppend(getValue(rlepos + offset), getLength(rlepos + offset));
        rlepos++;
      } else {
        smartAppend(i.next());
      }
    }
    if (i.hasNext()) {
      /*
       * if(this.nbrruns>0) { // this might be useful if the run mask has just one very large
       * run int lastval = Util.toIntUnsigned(getValue(nbrruns + offset - 1)) +
       * Util.toIntUnsigned(getLength(nbrruns + offset - 1)) + 1; i.advanceIfNeeded((short)
       * lastval); }
       */
      while (i.hasNext()) {
        smartAppend(i.next());
      }
    } else {
      while (rlepos < nbrruns) {
        smartAppend(getValue(rlepos + offset), getLength(rlepos + offset));
        rlepos++;
      }
    }
    return convertToLazyBitmapIfNeeded();
  }

  private void increaseCapacity() {
    int newCapacity = (valueslength.length == 0) ? DEFAULT_INIT_SIZE
            : valueslength.length < 64 ? valueslength.length * 2
            : valueslength.length < 1024 ? valueslength.length * 3 / 2
            : valueslength.length * 5 / 4;
    short[] nv = new short[newCapacity];
    System.arraycopy(valueslength, 0, nv, 0, 2 * nbrruns);
    valueslength = nv;
  }

  @Override
  public boolean intersects(SparseMask x) {
    if (this.nbrruns == 0) {
      return false;
    }
    int rlepos = 0;
    int arraypos = 0;
    int rleval = Util.toIntUnsigned(this.getValue(rlepos));
    int rlelength = Util.toIntUnsigned(this.getLength(rlepos));
    while (arraypos < x.cardinality) {
      int arrayval = Util.toIntUnsigned(x.content[arraypos]);
      while (rleval + rlelength < arrayval) {// this will frequently be false
        ++rlepos;
        if (rlepos == this.nbrruns) {
          return false;
        }
        rleval = Util.toIntUnsigned(this.getValue(rlepos));
        rlelength = Util.toIntUnsigned(this.getLength(rlepos));
      }
      if (rleval > arrayval) {
        arraypos = Util.advanceUntil(x.content, arraypos, x.cardinality, this.getValue(rlepos));
      } else {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean intersects(DenseMask x) {
    // TODO: this is probably not optimally fast
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int runStart = Util.toIntUnsigned(this.getValue(rlepos));
      int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));
      for (int runValue = runStart; runValue <= runEnd; ++runValue) {
        if (x.contains((short) runValue)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean intersects(RunMask x) {
    int rlepos = 0;
    int xrlepos = 0;
    int start = Util.toIntUnsigned(this.getValue(rlepos));
    int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
    int xstart = Util.toIntUnsigned(x.getValue(xrlepos));
    int xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
    while ((rlepos < this.nbrruns) && (xrlepos < x.nbrruns)) {
      if (end <= xstart) {
        if (ENABLE_GALLOPING_AND) {
          rlepos = skipAhead(this, rlepos, xstart); // skip over runs until we have end > xstart (or
          // rlepos is advanced beyond end)
        } else {
          ++rlepos;
        }

        if (rlepos < this.nbrruns) {
          start = Util.toIntUnsigned(this.getValue(rlepos));
          end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
      } else if (xend <= start) {
        // exit the second run
        if (ENABLE_GALLOPING_AND) {
          xrlepos = skipAhead(x, xrlepos, start);
        } else {
          ++xrlepos;
        }

        if (xrlepos < x.nbrruns) {
          xstart = Util.toIntUnsigned(x.getValue(xrlepos));
          xend = xstart + Util.toIntUnsigned(x.getLength(xrlepos)) + 1;
        }
      } else {// they overlap
        return true;
      }
    }
    return false;
  }

  protected boolean isFull() {
    return (this.nbrruns == 1) && (this.getValue(0) == 0) && (this.getLength(0) == -1);
  }

  public static Mask full() {
    return new RunMask(1, new short[]{0, -1});
  }

  private RunMask lazyandNot(SparseMask x) {
    if (x.getCardinality() == 0) {
      return this;
    }
    RunMask answer = new RunMask(new short[2 * (this.nbrruns + x.cardinality)], 0);
    int rlepos = 0;
    int xrlepos = 0;
    int start = Util.toIntUnsigned(this.getValue(rlepos));
    int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
    int xstart = Util.toIntUnsigned(x.content[xrlepos]);
    while ((rlepos < this.nbrruns) && (xrlepos < x.cardinality)) {
      if (end <= xstart) {
        // output the first run
        answer.valueslength[2 * answer.nbrruns] = (short) start;
        answer.valueslength[2 * answer.nbrruns + 1] = (short) (end - start - 1);
        answer.nbrruns++;
        rlepos++;
        if (rlepos < this.nbrruns) {
          start = Util.toIntUnsigned(this.getValue(rlepos));
          end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
        }
      } else if (xstart + 1 <= start) {
        // exit the second run
        xrlepos++;
        if (xrlepos < x.cardinality) {
          xstart = Util.toIntUnsigned(x.content[xrlepos]);
        }
      } else {
        if (start < xstart) {
          answer.valueslength[2 * answer.nbrruns] = (short) start;
          answer.valueslength[2 * answer.nbrruns + 1] = (short) (xstart - start - 1);
          answer.nbrruns++;
        }
        if (xstart + 1 < end) {
          start = xstart + 1;
        } else {
          rlepos++;
          if (rlepos < this.nbrruns) {
            start = Util.toIntUnsigned(this.getValue(rlepos));
            end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
          }
        }
      }
    }
    if (rlepos < this.nbrruns) {
      answer.valueslength[2 * answer.nbrruns] = (short) start;
      answer.valueslength[2 * answer.nbrruns + 1] = (short) (end - start - 1);
      answer.nbrruns++;
      rlepos++;
      if (rlepos < this.nbrruns) {
        System.arraycopy(this.valueslength, 2 * rlepos, answer.valueslength, 2 * answer.nbrruns,
                2 * (this.nbrruns - rlepos));
        answer.nbrruns = answer.nbrruns + this.nbrruns - rlepos;
      }
    }
    return answer;
  }

  protected Mask lazyor(SparseMask x) {
    return lazyorToRun(x);
  }

  private Mask lazyorToRun(SparseMask x) {
    if (isFull()) {
      return full();
    }
    // TODO: should optimize for the frequent case where we have a single run
    RunMask answer = new RunMask(new short[2 * (this.nbrruns + x.getCardinality())], 0);
    int rlepos = 0;
    MaskIterator i = x.iterator();

    while (i.hasNext() && (rlepos < this.nbrruns)) {
      if (Util.compareUnsigned(getValue(rlepos), i.peekNext()) <= 0) {
        answer.smartAppend(getValue(rlepos), getLength(rlepos));
        // in theory, this next code could help, in practice it doesn't.
        /*
         * int lastval = Util.toIntUnsigned(answer.getValue(answer.nbrruns - 1)) +
         * Util.toIntUnsigned(answer.getLength(answer.nbrruns - 1)) + 1; i.advanceIfNeeded((short)
         * lastval);
         */

        rlepos++;
      } else {
        answer.smartAppend(i.next());
      }
    }
    if (i.hasNext()) {
      /*
       * if(answer.nbrruns>0) { this might be useful if the run mask has just one very large
       * run int lastval = Util.toIntUnsigned(answer.getValue(answer.nbrruns - 1)) +
       * Util.toIntUnsigned(answer.getLength(answer.nbrruns - 1)) + 1; i.advanceIfNeeded((short)
       * lastval); }
       */
      while (i.hasNext()) {
        answer.smartAppend(i.next());
      }
    } else {
      while (rlepos < this.nbrruns) {
        answer.smartAppend(getValue(rlepos), getLength(rlepos));
        rlepos++;
      }
    }
    if (answer.isFull()) {
      return full();
    }
    return answer.convertToLazyBitmapIfNeeded();
  }

  private Mask lazyxor(SparseMask x) {
    if (x.getCardinality() == 0) {
      return this;
    }
    if (this.nbrruns == 0) {
      return x;
    }
    RunMask answer = new RunMask(new short[2 * (this.nbrruns + x.getCardinality())], 0);
    int rlepos = 0;
    MaskIterator i = x.iterator();
    short cv = i.next();

    while (true) {
      if (Util.compareUnsigned(getValue(rlepos), cv) < 0) {
        answer.smartAppendExclusive(getValue(rlepos), getLength(rlepos));
        rlepos++;
        if (rlepos == this.nbrruns) {
          answer.smartAppendExclusive(cv);
          while (i.hasNext()) {
            answer.smartAppendExclusive(i.next());
          }
          break;
        }
      } else {
        answer.smartAppendExclusive(cv);
        if (!i.hasNext()) {
          while (rlepos < this.nbrruns) {
            answer.smartAppendExclusive(getValue(rlepos), getLength(rlepos));
            rlepos++;
          }
          break;
        } else {
          cv = i.next();
        }
      }
    }
    return answer;
  }


  @Override
  public Mask limit(int maxcardinality) {
    if (maxcardinality >= getCardinality()) {
      return clone();
    }

    int r;
    int cardinality = 0;
    for (r = 0; r < this.nbrruns; ++r) {
      cardinality += Util.toIntUnsigned(getLength(r)) + 1;
      if (maxcardinality <= cardinality) {
        break;
      }
    }

    RunMask rc = new RunMask(Arrays.copyOf(valueslength, 2 * (r+1)), r+1);
    rc.setLength(r ,
            (short) (Util.toIntUnsigned(rc.getLength(r)) - cardinality + maxcardinality));
    return rc;
  }

  @Override
  public Mask not(int rangeStart, int rangeEnd) {
    if (rangeEnd <= rangeStart) {
      return this.clone();
    }
    RunMask ans = new RunMask(nbrruns + 1);
    int k = 0;
    for (; (k < this.nbrruns) && (Util.toIntUnsigned(this.getValue(k)) < rangeStart); ++k) {
      ans.valueslength[2 * k] = this.valueslength[2 * k];
      ans.valueslength[2 * k + 1] = this.valueslength[2 * k + 1];
      ans.nbrruns++;
    }
    ans.smartAppendExclusive((short) rangeStart, (short) (rangeEnd - rangeStart - 1));
    for (; k < this.nbrruns; ++k) {
      ans.smartAppendExclusive(getValue(k), getLength(k));
    }
    // the number of runs can increase by one, meaning (rarely) a bitmap will become better
    // or the cardinality can decrease by a lot, making an array better
    return ans.toEfficientMask();
  }

  @Override
  public int numberOfRuns() {
    return nbrruns;
  }

  @Override
  public Mask or(SparseMask x) {
    // we guess that, often, the result will still be efficiently expressed as a run mask
    return lazyor(x).repairAfterLazy();
  }

  @Override
  public Mask or(DenseMask x) {
    if (isFull()) {
      return full();
    }
    // could be implemented as return toTemporaryBitmap().ior(x);
    DenseMask answer = x.clone();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(this.getValue(rlepos));
      int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
      int prevOnesInRange = answer.cardinalityInRange(start, end);
      Util.setBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnesInRange, end - start);
    }
    if (answer.isFull()) {
      return full();
    }
    return answer;
  }

  @Override
  public Mask or(RunMask x) {
    if (isFull()) {
      return full();
    }
    if (x.isFull()) {
      return full(); // cheap case that can save a lot of computation
    }
    // we really ought to optimize the rest of the code for the frequent case where there is a
    // single run
    RunMask answer = new RunMask(new short[2 * (this.nbrruns + x.nbrruns)], 0);
    int rlepos = 0;
    int xrlepos = 0;

    while ((xrlepos < x.nbrruns) && (rlepos < this.nbrruns)) {
      if (Util.compareUnsigned(getValue(rlepos), x.getValue(xrlepos)) <= 0) {
        answer.smartAppend(getValue(rlepos), getLength(rlepos));
        rlepos++;
      } else {
        answer.smartAppend(x.getValue(xrlepos), x.getLength(xrlepos));
        xrlepos++;
      }
    }
    while (xrlepos < x.nbrruns) {
      answer.smartAppend(x.getValue(xrlepos), x.getLength(xrlepos));
      xrlepos++;
    }
    while (rlepos < this.nbrruns) {
      answer.smartAppend(getValue(rlepos), getLength(rlepos));
      rlepos++;
    }
    if (answer.isFull()) {
      return full();
    }
    return answer.toBitmapIfNeeded();
  }

  @Override
  public Mask repairAfterLazy() {
    return toEfficientMask();
  }

  private void setLength(int index, short v) {
    setLength(valueslength, index, v);
  }



  private void setLength(short[] valueslength, int index, short v) {
    valueslength[2 * index + 1] = v;
  }

  private void setValue(int index, short v) {
    setValue(valueslength, index, v);
  }

  private void setValue(short[] valueslength, int index, short v) {
    valueslength[2 * index] = v;
  }



  // bootstrapping (aka "galloping") binary search. Always skips at least one.
  // On our "real data" benchmarks, enabling galloping is a minor loss
  // .."ifdef ENABLE_GALLOPING_AND" :)
  private int skipAhead(RunMask skippingOn, int pos, int targetToExceed) {
    int left = pos;
    int span = 1;
    int probePos = 0;
    int end;
    // jump ahead to find a spot where end > targetToExceed (if it exists)
    do {
      probePos = left + span;
      if (probePos >= skippingOn.nbrruns - 1) {
        // expect it might be quite common to find the mask cannot be advanced as far as
        // requested. Optimize for it.
        probePos = skippingOn.nbrruns - 1;
        end = Util.toIntUnsigned(skippingOn.getValue(probePos))
                + Util.toIntUnsigned(skippingOn.getLength(probePos)) + 1;
        if (end <= targetToExceed) {
          return skippingOn.nbrruns;
        }
      }
      end = Util.toIntUnsigned(skippingOn.getValue(probePos))
              + Util.toIntUnsigned(skippingOn.getLength(probePos)) + 1;
      span *= 2;
    } while (end <= targetToExceed);
    int right = probePos;
    // left and right are both valid positions. Invariant: left <= targetToExceed && right >
    // targetToExceed
    // do a binary search to discover the spot where left and right are separated by 1, and
    // invariant is maintained.
    while (right - left > 1) {
      int mid = (right + left) / 2;
      int midVal = Util.toIntUnsigned(skippingOn.getValue(mid))
              + Util.toIntUnsigned(skippingOn.getLength(mid)) + 1;
      if (midVal > targetToExceed) {
        right = mid;
      } else {
        left = mid;
      }
    }
    return right;
  }

  private void smartAppend(short val) {
    int oldend;
    if ((nbrruns == 0)
            || (Util.toIntUnsigned(val) > (oldend = Util.toIntUnsigned(valueslength[2 * (nbrruns - 1)])
            + Util.toIntUnsigned(valueslength[2 * (nbrruns - 1) + 1])) + 1)) { // we add a new one
      valueslength[2 * nbrruns] = val;
      valueslength[2 * nbrruns + 1] = 0;
      nbrruns++;
      return;
    }
    if (val == (short) (oldend + 1)) { // we merge
      valueslength[2 * (nbrruns - 1) + 1]++;
    }
  }

  private void smartAppend(short start, short length) {
    int oldend;
    if ((nbrruns == 0) || (Util.toIntUnsigned(start) > (oldend =
            Util.toIntUnsigned(getValue(nbrruns - 1)) + Util.toIntUnsigned(getLength(nbrruns - 1)))
            + 1)) { // we add a new one
      valueslength[2 * nbrruns] = start;
      valueslength[2 * nbrruns + 1] = length;
      nbrruns++;
      return;
    }
    int newend = Util.toIntUnsigned(start) + Util.toIntUnsigned(length) + 1;
    if (newend > oldend) { // we merge
      setLength(nbrruns - 1, (short) (newend - 1 - Util.toIntUnsigned(getValue(nbrruns - 1))));
    }
  }

  private void smartAppendExclusive(short val) {
    int oldend;
    if ((nbrruns == 0)
            || (Util.toIntUnsigned(val) > (oldend = Util.toIntUnsigned(getValue(nbrruns - 1))
            + Util.toIntUnsigned(getLength(nbrruns - 1)) + 1))) { // we add a new one
      valueslength[2 * nbrruns] = val;
      valueslength[2 * nbrruns + 1] = 0;
      nbrruns++;
      return;
    }
    if (oldend == Util.toIntUnsigned(val)) {
      // we merge
      valueslength[2 * (nbrruns - 1) + 1]++;
      return;
    }
    int newend = Util.toIntUnsigned(val) + 1;

    if (Util.toIntUnsigned(val) == Util.toIntUnsigned(getValue(nbrruns - 1))) {
      // we wipe out previous
      if (newend != oldend) {
        setValue(nbrruns - 1, (short) newend);
        setLength(nbrruns - 1, (short) (oldend - newend - 1));
        return;
      } else { // they cancel out
        nbrruns--;
        return;
      }
    }
    setLength(nbrruns - 1, (short) (val - Util.toIntUnsigned(getValue(nbrruns - 1)) - 1));
    if (newend < oldend) {
      setValue(nbrruns, (short) newend);
      setLength(nbrruns, (short) (oldend - newend - 1));
      nbrruns++;
    } else if (oldend < newend) {
      setValue(nbrruns, (short) oldend);
      setLength(nbrruns, (short) (newend - oldend - 1));
      nbrruns++;
    }

  }

  private void smartAppendExclusive(short start, short length) {
    int oldend;
    if ((nbrruns == 0)
            || (Util.toIntUnsigned(start) > (oldend = Util.toIntUnsigned(getValue(nbrruns - 1))
            + Util.toIntUnsigned(getLength(nbrruns - 1)) + 1))) { // we add a new one
      valueslength[2 * nbrruns] = start;
      valueslength[2 * nbrruns + 1] = length;
      nbrruns++;
      return;
    }
    if (oldend == Util.toIntUnsigned(start)) {
      // we merge
      valueslength[2 * (nbrruns - 1) + 1] += length + 1;
      return;
    }

    int newend = Util.toIntUnsigned(start) + Util.toIntUnsigned(length) + 1;

    if (Util.toIntUnsigned(start) == Util.toIntUnsigned(getValue(nbrruns - 1))) {
      // we wipe out previous
      if (newend < oldend) {
        setValue(nbrruns - 1, (short) newend);
        setLength(nbrruns - 1, (short) (oldend - newend - 1));
        return;
      } else if (newend > oldend) {
        setValue(nbrruns - 1, (short) oldend);
        setLength(nbrruns - 1, (short) (newend - oldend - 1));
        return;
      } else { // they cancel out
        nbrruns--;
        return;
      }
    }
    setLength(nbrruns - 1, (short) (start - Util.toIntUnsigned(getValue(nbrruns - 1)) - 1));
    if (newend < oldend) {
      setValue(nbrruns, (short) newend);
      setLength(nbrruns, (short) (oldend - newend - 1));
      nbrruns++;
    } else if (newend > oldend) {
      setValue(nbrruns, (short) oldend);
      setLength(nbrruns, (short) (newend - oldend - 1));
      nbrruns++;
    }
  }

  // convert to bitmap *if needed* (useful if you know it can't be an array)
  private Mask toBitmapIfNeeded() {
    int sizeAsRunMask = RunMask.serializedSizeInBytes(this.nbrruns);
    int sizeAsBitmapMask = DenseMask.serializedSizeInBytes();
    if (sizeAsBitmapMask > sizeAsRunMask) {
      return this;
    }
    return toBitmapMask();
  }

  /**
   * Convert the mask to either a Bitmap or an Array mask, depending on the cardinality.
   *
   * @param card the current cardinality
   * @return new mask
   */
  Mask toBitmapOrArrayMask(int card) {
    // int card = this.getCardinality();
    if (card <= SparseMask.DEFAULT_MAX_SIZE) {
      SparseMask answer = new SparseMask(card);
      answer.cardinality = 0;
      for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
        int runStart = Util.toIntUnsigned(this.getValue(rlepos));
        int runEnd = runStart + Util.toIntUnsigned(this.getLength(rlepos));

        for (int runValue = runStart; runValue <= runEnd; ++runValue) {
          answer.content[answer.cardinality++] = (short) runValue;
        }
      }
      return answer;
    }
    DenseMask answer = new DenseMask();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(this.getValue(rlepos));
      int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
      Util.setBitmapRange(answer.bitmap, start, end);
    }
    answer.cardinality = card;
    return answer;
  }

  // convert to bitmap or array *if needed*
  private Mask toEfficientMask() {
    int sizeAsRunMask = RunMask.serializedSizeInBytes(this.nbrruns);
    int sizeAsBitmapMask = DenseMask.serializedSizeInBytes();
    int card = this.getCardinality();
    int sizeAsArrayMask = SparseMask.serializedSizeInBytes(card);
    if (sizeAsRunMask <= Math.min(sizeAsBitmapMask, sizeAsArrayMask)) {
      return this;
    }
    return toBitmapOrArrayMask(card);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < this.nbrruns; ++k) {
      sb.append("[");
      sb.append(Util.toIntUnsigned(this.getValue(k)));
      sb.append(",");
      sb.append(Util.toIntUnsigned(this.getValue(k)) + Util.toIntUnsigned(this.getLength(k)));
      sb.append("]");
    }
    return sb.toString();
  }

  @Override
  public Mask xor(SparseMask x) {
    // if the cardinality of the array is small, guess that the output will still be a run mask
    final int arbitrary_threshold = 32; // 32 is arbitrary here
    if (x.getCardinality() < arbitrary_threshold) {
      return lazyxor(x).repairAfterLazy();
    }
    // otherwise, we expect the output to be either an array or bitmap
    final int card = getCardinality();
    if (card <= SparseMask.DEFAULT_MAX_SIZE) {
      // if the cardinality is small, we construct the solution in place
      return x.xor(this.iterator());
    }
    // otherwise, we generate a bitmap (even if runmask would be better)
    return toBitmapOrArrayMask(card).xor(x);
  }

  @Override
  public Mask xor(DenseMask x) {
    // could be implemented as return toTemporaryBitmap().ixor(x);
    DenseMask answer = x.clone();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(this.getValue(rlepos));
      int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
      int prevOnes = answer.cardinalityInRange(start, end);
      Util.flipBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnes, end - start - prevOnes);
    }
    if (answer.getCardinality() > SparseMask.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayMask();
    }
  }

  @Override
  public Mask xor(RunMask x) {
    if (x.nbrruns == 0) {
      return this.clone();
    }
    if (this.nbrruns == 0) {
      return x.clone();
    }
    RunMask answer = new RunMask(new short[2 * (this.nbrruns + x.nbrruns)], 0);
    int rlepos = 0;
    int xrlepos = 0;

    while (true) {
      if (Util.compareUnsigned(getValue(rlepos), x.getValue(xrlepos)) < 0) {
        answer.smartAppendExclusive(getValue(rlepos), getLength(rlepos));
        rlepos++;

        if (rlepos == this.nbrruns) {
          while (xrlepos < x.nbrruns) {
            answer.smartAppendExclusive(x.getValue(xrlepos), x.getLength(xrlepos));
            xrlepos++;
          }
          break;
        }
      } else {
        answer.smartAppendExclusive(x.getValue(xrlepos), x.getLength(xrlepos));

        xrlepos++;
        if (xrlepos == x.nbrruns) {
          while (rlepos < this.nbrruns) {
            answer.smartAppendExclusive(getValue(rlepos), getLength(rlepos));
            rlepos++;
          }
          break;
        }
      }
    }
    return answer.toEfficientMask();
  }

  @Override
  public DenseMask toBitmapMask() {
    int card = this.getCardinality();
    DenseMask answer = new DenseMask();
    for (int rlepos = 0; rlepos < this.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(this.getValue(rlepos));
      int end = start + Util.toIntUnsigned(this.getLength(rlepos)) + 1;
      Util.setBitmapRange(answer.bitmap, start, end);
    }
    answer.cardinality = card;
    return answer;
  }

  @Override
  public int first() {
    assertNonEmpty(numberOfRuns() == 0);
    return Util.toIntUnsigned(valueslength[0]);
  }

  @Override
  public int last() {
    assertNonEmpty(numberOfRuns() == 0);
    int index = numberOfRuns() - 1;
    int start = Util.toIntUnsigned(getValue(index));
    int length = Util.toIntUnsigned(getLength(index));
    return start + length;
  }

}


