package com.openkappa.splitmap;

public class Reduction {

  public static double[] sum(double[] l, double[] r) {
    if (null == l) {
      return r;
    }
    if (null == r) {
      return l;
    }
    sumRightIntoLeft(l, r);
    return l;
  }

  public static void sumRightIntoLeft(double[] l, double[] r) {
    if (l.length != r.length) {
      throw new IllegalStateException("array lengths must match");
    }
    for (int i = 0; i < l.length && i < r.length; ++i) {
      l[i] += r[i];
    }
  }

  public static double add(double x, double y) {
    return x + y;
  }
}
