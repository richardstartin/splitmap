package com.openkappa.splitmap;

public class Reduction {

  public static double[] sum(double[] l, double[] r) {
    if (null == l) {
      return r;
    }
    if (null == r) {
      return l;
    }
    return sumRightIntoLeft(l, r);
  }

  public static double[] sumRightIntoLeft(double[] l, double[] r) {
    if (l.length != r.length) {
      throw new IllegalStateException("array lengths must match");
    }
    for (int i = 0; i < l.length && i < r.length; ++i) {
      l[i] += r[i];
    }
    return l;
  }


  public static double horizontalSum(double[] vector) {
    double sum1 = 0D;
    double sum2 = 0D;
    double sum3 = 0D;
    double sum4 = 0D;
    int i = 0;
    for (; i + 4 < vector.length; i += 4) {
      sum1 += vector[i];
      sum2 += vector[i + 1];
      sum3 += vector[i + 2];
      sum4 += vector[i + 3];
    }
    for (; i < vector.length; ++i) {
      sum1 += vector[i];
    }
    return sum1 + sum2 + sum3 + sum4;
  }

  public static double add(double x, double y) {
    return x + y;
  }
}
