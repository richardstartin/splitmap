package com.openkappa.splitmap;

public class Reducers {

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
}
