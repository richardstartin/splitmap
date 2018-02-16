/*
 * Code taken and modified from RoaringBitmap project with the copyright notice below:
 *
 * (c) the authors Licensed under the Apache License, Version 2.0.
 * (https://github.com/RoaringBitmap/RoaringBitmap/blob/master/AUTHORS)
 */

package com.openkappa.splitmap.roaring;

import java.util.NoSuchElementException;

/**
 * Base mask class.
 */
public abstract class Mask implements Cloneable {

  /**
   * Computes the bitwise AND of this mask with another (intersection). This mask as well
   * as the provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask and(SparseMask x);

  /**
   * Computes the bitwise AND of this mask with another (intersection). This mask as well
   * as the provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask and(DenseMask x);

  /**
   * Computes the bitwise AND of this mask with another (intersection). This mask as well
   * as the provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public Mask and(Mask x) {
    if (x instanceof SparseMask) {
      return and((SparseMask) x);
    } else if (x instanceof DenseMask) {
      return and((DenseMask) x);
    }
    return and((RunMask) x);
  }


  /**
   * Computes the bitwise AND of this mask with another (intersection). This mask as well
   * as the provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask and(RunMask x);


  /**
   * Computes the bitwise ANDNOT of this mask with another (difference). This mask as well
   * as the provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask andNot(SparseMask x);

  /**
   * Computes the bitwise ANDNOT of this mask with another (difference). This mask as well
   * as the provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask andNot(DenseMask x);

  /**
   * Computes the bitwise ANDNOT of this mask with another (difference). This mask as well
   * as the provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public Mask andNot(Mask x) {
    if (x instanceof SparseMask) {
      return andNot((SparseMask) x);
    } else if (x instanceof DenseMask) {
      return andNot((DenseMask) x);
    }
    return andNot((RunMask) x);
  }


  /**
   * Computes the bitwise ANDNOT of this mask with another (difference). This mask as well
   * as the provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask andNot(RunMask x);

  @Override
  public abstract Mask clone();

  /**
   * Checks whether the contain contains the provided value
   *
   * @param x value to check
   * @return whether the value is in the mask
   */
  public abstract boolean contains(short x);


  /**
   * Checks whether the mask is a subset of this mask or not
   * @param subset the mask to be tested
   * @return true if the parameter is a subset of this mask
   */
  public boolean contains(Mask subset) {
    if(subset instanceof RunMask) {
      return contains((RunMask)subset);
    } else if(subset instanceof SparseMask) {
      return contains((SparseMask) subset);
    } else if(subset instanceof DenseMask){
      return contains((DenseMask)subset);
    }
    return false;
  }


  protected abstract boolean contains(RunMask runMask);

  protected abstract boolean contains(SparseMask sparseMask);

  protected abstract boolean contains(DenseMask denseMask);

  /**
   * Computes the distinct number of short values in the mask. Can be expected to run in
   * constant time.
   *
   * @return the cardinality
   */
  public abstract int getCardinality();


  /**
   * Iterator to visit the short values in the mask in ascending order.
   *
   * @return iterator
   */
  public abstract MaskIterator iterator();

  /**
   * Returns true if the current mask intersects the other mask.
   *
   * @param x other mask
   * @return whether they intersect
   */
  public abstract boolean intersects(SparseMask x);

  /**
   * Returns true if the current mask intersects the other mask.
   *
   * @param x other mask
   * @return whether they intersect
   */
  public abstract boolean intersects(DenseMask x);

  /**
   * Returns true if the current mask intersects the other mask.
   *
   * @param x other mask
   * @return whether they intersect
   */
  public boolean intersects(Mask x) {
    if (x instanceof SparseMask) {
      return intersects((SparseMask) x);
    } else if (x instanceof DenseMask) {
      return intersects((DenseMask) x);
    }
    return intersects((RunMask) x);
  }

  /**
   * Returns true if the current mask intersects the other mask.
   *
   * @param x other mask
   * @return whether they intersect
   */
  public abstract boolean intersects(RunMask x);


  /**
   * Create a new Mask containing at most maxcardinality integers.
   *
   * @param maxcardinality maximal cardinality
   * @return a new bitmap with cardinality no more than maxcardinality
   */
  public abstract Mask limit(int maxcardinality);

  /**
   * Computes the bitwise NOT of this mask (complement). Only those bits within the range are
   * affected. The current mask is left unaffected.
   *
   * @param rangeStart beginning of range (inclusive); 0 is beginning of this mask.
   * @param rangeEnd ending of range (exclusive)
   * @return (partially) complemented mask
   */
  public abstract Mask not(int rangeStart, int rangeEnd);

  abstract int numberOfRuns(); // exact


  /**
   * Computes the bitwise OR of this mask with another (union). This mask as well as the
   * provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask or(SparseMask x);

  /**
   * Computes the bitwise OR of this mask with another (union). This mask as well as the
   * provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask or(DenseMask x);

  /**
   * Computes the bitwise OR of this mask with another (union). This mask as well as the
   * provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public Mask or(Mask x) {
    if (x instanceof SparseMask) {
      return or((SparseMask) x);
    } else if (x instanceof DenseMask) {
      return or((DenseMask) x);
    }
    return or((RunMask) x);
  }

  /**
   * Computes the bitwise OR of this mask with another (union). This mask as well as the
   * provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask or(RunMask x);


  /**
   * The output of a lazyOR or lazyIOR might be an invalid mask, this should be called on it.
   *
   * @return a new valid mask
   */
  public abstract Mask repairAfterLazy();



  /**
   * Computes the bitwise XOR of this mask with another (symmetric difference). This mask
   * as well as the provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask xor(SparseMask x);

  /**
   * Computes the bitwise XOR of this mask with another (symmetric difference). This mask
   * as well as the provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask xor(DenseMask x);


  /**
   * Computes the bitwise OR of this mask with another (symmetric difference). This mask
   * as well as the provided mask are left unaffected.
   *
   * @param x other parameter
   * @return aggregated mask
   */
  public Mask xor(Mask x) {
    if (x instanceof SparseMask) {
      return xor((SparseMask) x);
    } else if (x instanceof DenseMask) {
      return xor((DenseMask) x);
    }
    return xor((RunMask) x);
  }

  /**
   * Computes the bitwise XOR of this mask with another (symmetric difference). This mask
   * as well as the provided mask are left unaffected.
   *
   * @param x other mask
   * @return aggregated mask
   */
  public abstract Mask xor(RunMask x);

  /**
   * Convert the current mask to a DenseMask, if a conversion is needed.
   * If the mask is already a bitmap, the mask is returned unchanged.
   * @return a bitmap mask
   */
  public abstract DenseMask toBitmapMask();

  /**
   * Get the first integer held in the mask
   * @return the first integer in the mask
   * @throws NoSuchElementException if empty
   */
  public abstract int first();

  /**
   * Get the last integer held in the mask
   * @return the last integer in the mask
   * @throws NoSuchElementException if empty
   */
  public abstract int last();

  /**
   * Throw if the mask is empty
   * @param condition a boolean expression
   * @throws NoSuchElementException if empty
   */
  protected void assertNonEmpty(boolean condition) {
    if(condition) {
      throw new NoSuchElementException("Empty");
    }
  }
}
