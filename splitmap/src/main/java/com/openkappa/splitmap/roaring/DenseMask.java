/*
 * Code taken and modified from RoaringBitmap project with the copyright notice below:
 *
 * (c) the authors Licensed under the Apache License, Version 2.0.
 * (https://github.com/RoaringBitmap/RoaringBitmap/blob/master/AUTHORS)
 */

package com.openkappa.splitmap.roaring;

import java.util.Arrays;


/**
 * Simple bitset-like mask.
 */
public final class DenseMask extends Mask implements Cloneable {
  protected static final int MAX_CAPACITY = 1 << 16;

  // 64 words can have max 32 runs per word, max 2k runs

  /**
   * optimization flag: whether the cardinality of the bitmaps is maintained through branchless
   * operations
   */
  public static final boolean USE_BRANCHLESS = true;

  // the parameter is for overloading and symmetry with SparseMask
  protected static int serializedSizeInBytes() {
    return MAX_CAPACITY / 8;
  }

  final long[] bitmap;

  int cardinality;

  // nruns value for which RunMask.serializedSizeInBytes ==
  // DenseMask.getArraySizeInBytes()
  //private final int MAXRUNS = (getArraySizeInBytes() - 2) / 4;


  /**
   * Create a bitmap mask with all bits set to false
   */
  public DenseMask() {
    this.cardinality = 0;
    this.bitmap = new long[MAX_CAPACITY / 64];
  }



  /**
   * Create a bitmap mask with a run of ones from firstOfRun to lastOfRun. Caller must ensure
   * that the range isn't so small that an SparseMask should have been created instead
   *
   * @param firstOfRun first index
   * @param lastOfRun last index (range is exclusive)
   */
  public DenseMask(final int firstOfRun, final int lastOfRun) {
    this.cardinality = lastOfRun - firstOfRun;
    this.bitmap = new long[MAX_CAPACITY / 64];
    Util.setBitmapRange(bitmap, firstOfRun, lastOfRun);
  }

  private DenseMask(int newCardinality, long[] newBitmap) {
    this.cardinality = newCardinality;
    this.bitmap = Arrays.copyOf(newBitmap, newBitmap.length);
  }

  /**
   * Create a new mask, no copy is made.
   *
   * @param newBitmap content
   * @param newCardinality desired cardinality.
   */
  public DenseMask(long[] newBitmap, int newCardinality) {
    this.cardinality = newCardinality;
    this.bitmap = newBitmap;
  }

  @Override
  public SparseMask and(final SparseMask value2) {
    final SparseMask answer = new SparseMask(value2.content.length);
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v = value2.content[k];
      answer.content[answer.cardinality] = v;
      answer.cardinality += this.bitValue(v);
    }
    return answer;
  }

  @Override
  public Mask and(final DenseMask value2) {
    int newCardinality = 0;
    for (int k = 0; k < this.bitmap.length; ++k) {
      newCardinality += Long.bitCount(this.bitmap[k] & value2.bitmap[k]);
    }
    if (newCardinality > SparseMask.DEFAULT_MAX_SIZE) {
      final DenseMask answer = new DenseMask();
      for (int k = 0; k < answer.bitmap.length; ++k) {
        answer.bitmap[k] = this.bitmap[k] & value2.bitmap[k];
      }
      answer.cardinality = newCardinality;
      return answer;
    }
    SparseMask ac = new SparseMask(newCardinality);
    Util.fillArrayAND(ac.content, this.bitmap, value2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public Mask and(RunMask x) {
    return x.and(this);
  }

  @Override
  public Mask andNot(final SparseMask value2) {
    final DenseMask answer = clone();
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v = value2.content[k];
      final int i = Util.toIntUnsigned(v) >>> 6;
      long w = answer.bitmap[i];
      long aft = w & (~(1L << v));
      answer.bitmap[i] = aft;
      answer.cardinality -= (w ^ aft) >>> v;
    }
    if (answer.cardinality <= SparseMask.DEFAULT_MAX_SIZE) {
      return answer.toArrayMask();
    }
    return answer;
  }

  @Override
  public Mask andNot(final DenseMask value2) {
    int newCardinality = 0;
    for (int k = 0; k < this.bitmap.length; ++k) {
      newCardinality += Long.bitCount(this.bitmap[k] & (~value2.bitmap[k]));
    }
    if (newCardinality > SparseMask.DEFAULT_MAX_SIZE) {
      final DenseMask answer = new DenseMask();
      for (int k = 0; k < answer.bitmap.length; ++k) {
        answer.bitmap[k] = this.bitmap[k] & (~value2.bitmap[k]);
      }
      answer.cardinality = newCardinality;
      return answer;
    }
    SparseMask ac = new SparseMask(newCardinality);
    Util.fillArrayANDNOT(ac.content, this.bitmap, value2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public Mask not(final int firstOfRange, final int lastOfRange) {
    long[] bitmap = Arrays.copyOf(this.bitmap, this.bitmap.length);
    int prevOnes = cardinalityInRange(firstOfRange, lastOfRange);
    Util.flipBitmapRange(bitmap, firstOfRange, lastOfRange);
    int cardinality = this.cardinality - 2 * prevOnes + lastOfRange - firstOfRange;
    if (cardinality <= SparseMask.DEFAULT_MAX_SIZE) {
      return toArrayMask();
    }
    return new DenseMask(bitmap, cardinality);
  }

  @Override
  public Mask andNot(RunMask x) {
    // could be rewritten as return andNot(x.toBitmapOrArrayMask());
    DenseMask answer = this.clone();
    for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
      int start = Util.toIntUnsigned(x.getValue(rlepos));
      int end = start + Util.toIntUnsigned(x.getLength(rlepos)) + 1;
      int prevOnesInRange = answer.cardinalityInRange(start, end);
      Util.resetBitmapRange(answer.bitmap, start, end);
      answer.updateCardinality(prevOnesInRange, 0);
    }
    if (answer.getCardinality() > SparseMask.DEFAULT_MAX_SIZE) {
      return answer;
    } else {
      return answer.toArrayMask();
    }
  }

  @Override
  public DenseMask clone() {
    return new DenseMask(this.cardinality, this.bitmap);
  }

  /**
   * Recomputes the cardinality of the bitmap.
   */
  protected static int computeCardinality(long[] bitmap) {
    int cardinality = 0;
    for (int k = 0; k < bitmap.length; k++) {
      cardinality += Long.bitCount(bitmap[k]);
    }
    return cardinality;
  }

  protected int cardinalityInRange(int start, int end) {
    assert (cardinality != -1);
    if (end - start > MAX_CAPACITY / 2) {
      int before = Util.cardinalityInBitmapRange(bitmap, 0, start);
      int after = Util.cardinalityInBitmapRange(bitmap, end, MAX_CAPACITY);
      return cardinality - before - after;
    }
    return Util.cardinalityInBitmapRange(bitmap, start, end);
  }

  protected void updateCardinality(int prevOnes, int newOnes) {
    int oldCardinality = this.cardinality;
    this.cardinality = oldCardinality - prevOnes + newOnes;
  }

  @Override
  public boolean contains(final short i) {
    final int x = Util.toIntUnsigned(i);
    return (bitmap[x / 64] & (1L << x)) != 0;
  }

  @Override
  protected boolean contains(DenseMask denseMask) {
    if((cardinality != -1) && (denseMask.cardinality != -1)) {
      if(cardinality < denseMask.cardinality) {
        return false;
      }
    }
    for(int i = 0; i < denseMask.bitmap.length; ++i ) {
      if((this.bitmap[i] & denseMask.bitmap[i]) != denseMask.bitmap[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean contains(RunMask runMask) {
    final int runCardinality = runMask.getCardinality();
    if (cardinality != -1) {
      if (cardinality < runCardinality) {
        return false;
      }
    } else {
      int card = cardinality;
      if (card < runCardinality) {
        return false;
      }
    }
    for (int i = 0; i < runMask.numberOfRuns(); ++i) {
      short runStart = runMask.getValue(i);
      int le = Util.toIntUnsigned(runMask.getLength(i));
      for (short j = runStart; j <= runStart + le; ++j) {
        if (!contains(j)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  protected boolean contains(SparseMask sparseMask) {
    if (sparseMask.cardinality != -1) {
      if (cardinality < sparseMask.cardinality) {
        return false;
      }
    }
    for (int i = 0; i < sparseMask.cardinality; ++i) {
      if(!contains(sparseMask.content[i])) {
        return false;
      }
    }
    return true;
  }


  protected long bitValue(final short i) {
    final int x = Util.toIntUnsigned(i);
    return (bitmap[x / 64] >>> x ) & 1;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DenseMask) {
      DenseMask srb = (DenseMask) o;
      if (srb.cardinality != this.cardinality) {
        return false;
      }
      return Arrays.equals(this.bitmap, srb.bitmap);
    } else if (o instanceof RunMask) {
      return o.equals(this);
    }
    return false;
  }


  /**
   * Fill the array with set bits
   *
   * @param array container (should be sufficiently large)
   */
  protected void fillArray(final short[] array) {
    int pos = 0;
    int base = 0;
    for (int k = 0; k < bitmap.length; ++k) {
      long bitset = bitmap[k];
      while (bitset != 0) {
        long t = bitset & -bitset;
        array[pos++] = (short) (base + Long.bitCount(t - 1));
        bitset ^= t;
      }
      base += 64;
    }
  }

  @Override
  public int getCardinality() {
    return cardinality;
  }

  @Override
  public MaskIterator iterator() {
    return new DenseMaskIterator(this.bitmap);
  }


  @Override
  public int hashCode() {
    return Arrays.hashCode(this.bitmap);
  }


  @Override
  public boolean intersects(SparseMask value2) {
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      if (this.contains(value2.content[k])) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean intersects(DenseMask value2) {
    for (int k = 0; k < this.bitmap.length; ++k) {
      if ((this.bitmap[k] & value2.bitmap[k]) != 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean intersects(RunMask x) {
    return x.intersects(this);
  }

  @Override
  public Mask limit(int maxcardinality) {
    if (maxcardinality >= this.cardinality) {
      return clone();
    }
    if (maxcardinality <= SparseMask.DEFAULT_MAX_SIZE) {
      SparseMask ac = new SparseMask(maxcardinality);
      int pos = 0;
      for (int k = 0; (ac.cardinality < maxcardinality) && (k < bitmap.length); ++k) {
        long bitset = bitmap[k];
        while ((ac.cardinality < maxcardinality) && (bitset != 0)) {
          long t = bitset & -bitset;
          ac.content[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
          ac.cardinality++;
          bitset ^= t;
        }
      }
      return ac;
    }
    DenseMask bc = new DenseMask(maxcardinality, this.bitmap);
    int s = Util.toIntUnsigned(select(maxcardinality));
    int usedwords = (s + 63) / 64;
    int todelete = this.bitmap.length - usedwords;
    for (int k = 0; k < todelete; ++k) {
      bc.bitmap[bc.bitmap.length - 1 - k] = 0;
    }
    int lastword = s % 64;
    if (lastword != 0) {
      bc.bitmap[s / 64] &= (0xFFFFFFFFFFFFFFFFL >>> (64 - lastword));
    }
    return bc;
  }

  protected void loadData(final SparseMask sparseMask) {
    this.cardinality = sparseMask.cardinality;
    for (int k = 0; k < sparseMask.cardinality; ++k) {
      final short x = sparseMask.content[k];
      bitmap[Util.toIntUnsigned(x) / 64] |= (1L << x);
    }
  }

  @Override
  int numberOfRuns() {
    int numRuns = 0;
    long nextWord = bitmap[0];

    for (int i = 0; i < bitmap.length - 1; i++) {
      long word = nextWord;
      nextWord = bitmap[i + 1];
      numRuns += Long.bitCount((~word) & (word << 1)) + ((word >>> 63) & ~nextWord);
    }

    long word = nextWord;
    numRuns += Long.bitCount((~word) & (word << 1));
    if ((word & 0x8000000000000000L) != 0) {
      numRuns++;
    }

    return numRuns;
  }

  @Override
  public Mask or(final SparseMask value2) {
    final DenseMask answer = clone();
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short v = value2.content[k];
      final int i = Util.toIntUnsigned(v) >>> 6;
      long w = answer.bitmap[i];
      long aft = w | (1L << v);
      answer.bitmap[i] = aft;
      if (USE_BRANCHLESS) {
        answer.cardinality += (w - aft) >>> 63;
      } else {
        if (w != aft) {
          answer.cardinality++;
        }
      }
    }
    if (answer.isFull(cardinality)) {
      return RunMask.full();
    }
    return answer;
  }

  protected boolean isFull(int cardinality) {
    return cardinality == MAX_CAPACITY;
  }

  public boolean isFull() {
    return isFull(cardinality);
  }

  @Override
  public Mask or(final DenseMask value2) {
    long[] bitmap = Arrays.copyOf(this.bitmap, this.bitmap.length);
    for (int k = 0; k < bitmap.length; k++) {
      bitmap[k] |= value2.bitmap[k];
    }
    int cardinality = computeCardinality(bitmap);
    if (isFull(cardinality)) {
      return RunMask.full();
    }
    return new DenseMask(bitmap, cardinality);
  }

  @Override
  public Mask or(RunMask x) {
    return x.or(this);
  }

  @Override
  public Mask repairAfterLazy() {
    if (getCardinality() < 0) {
      this.cardinality = computeCardinality(bitmap);
      if(getCardinality() <= SparseMask.DEFAULT_MAX_SIZE) {
        return this.toArrayMask();
      } else if (isFull(cardinality)) {
        return RunMask.full();
      }
    }
    return this;
  }


  private short select(int j) {
    int leftover = j;
    for (int k = 0; k < bitmap.length; ++k) {
      int w = Long.bitCount(bitmap[k]);
      if (w > leftover) {
        return (short) (k * 64 + Util.select(bitmap[k], leftover));
      }
      leftover -= w;
    }
    throw new IllegalArgumentException("Insufficient cardinality.");
  }

  /**
   * Copies the data to an array container
   *
   * @return the array container
   */
  public SparseMask toArrayMask() {
    SparseMask ac = new SparseMask(cardinality);
    ac.loadData(this);
    if (ac.getCardinality() != cardinality) {
      throw new RuntimeException("Internal error.");
    }
    return ac;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    final MaskIterator i = this.iterator();
    sb.append("{");
    while (i.hasNext()) {
      sb.append(i.nextAsInt());
      if (i.hasNext()) {
        sb.append(",");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  @Override
  public Mask xor(final SparseMask value2) {
    final DenseMask answer = clone();
    int c = value2.cardinality;
    for (int k = 0; k < c; ++k) {
      short vc = value2.content[k];
      final int index = Util.toIntUnsigned(vc) >>> 6;
      final long mask = 1L << vc;
      final long val = answer.bitmap[index];
      // TODO: check whether a branchy version could be faster
      answer.cardinality += 1 - 2 * ((val & mask) >>> vc);
      answer.bitmap[index] = val ^ mask;
    }
    if (answer.cardinality <= SparseMask.DEFAULT_MAX_SIZE) {
      return answer.toArrayMask();
    }
    return answer;
  }

  @Override
  public Mask xor(DenseMask value2) {
    int newCardinality = 0;
    for (int k = 0; k < this.bitmap.length; ++k) {
      newCardinality += Long.bitCount(this.bitmap[k] ^ value2.bitmap[k]);
    }
    if (newCardinality > SparseMask.DEFAULT_MAX_SIZE) {
      final DenseMask answer = new DenseMask();
      for (int k = 0; k < answer.bitmap.length; ++k) {
        answer.bitmap[k] = this.bitmap[k] ^ value2.bitmap[k];
      }
      answer.cardinality = newCardinality;
      return answer;
    }
    SparseMask ac = new SparseMask(newCardinality);
    Util.fillArrayXOR(ac.content, this.bitmap, value2.bitmap);
    ac.cardinality = newCardinality;
    return ac;
  }

  @Override
  public Mask xor(RunMask x) {
    return x.xor(this);
  }

  @Override
  public DenseMask toBitmapMask() {
    return this;
  }

  @Override
  public int first() {
    assertNonEmpty(cardinality == 0);
    int i = 0;
    while(i < bitmap.length - 1 && bitmap[i] == 0) {
      ++i; // seek forward
    }
    // sizeof(long) * #empty words at start + number of bits preceding the first bit set
    return i * 64 + Long.numberOfTrailingZeros(bitmap[i]);
  }

  @Override
  public int last() {
    assertNonEmpty(cardinality == 0);
    int i = bitmap.length - 1;
    while(i > 0 && bitmap[i] == 0) {
      --i; // seek backward
    }
    // sizeof(long) * #words from start - number of bits after the last bit set
    return (i + 1) * 64 - Long.numberOfLeadingZeros(bitmap[i]) - 1;
  }

}


