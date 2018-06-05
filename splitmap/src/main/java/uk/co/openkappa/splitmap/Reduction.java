package uk.co.openkappa.splitmap;

import static uk.co.openkappa.splitmap.Vectors.D256;

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
    var sum1 = D256.zero();
    var sum2 = D256.zero();
    var sum3 = D256.zero();
    var sum4 = D256.zero();
    int i = 0;
    for (; i < vector.length; i += 4 * D256.length()) {
      sum1 = sum1.add(D256.fromArray(vector, i));
      sum2 = sum2.add(D256.fromArray(vector, i + D256.length()));
      sum3 = sum3.add(D256.fromArray(vector, i + 2 * D256.length()));
      sum4 = sum4.add(D256.fromArray(vector, i + 3 * D256.length()));
    }
    double residue = 0D;
    for (; i < vector.length; ++i) {
      residue += vector[i];
    }
    return sum1.addAll() + sum2.addAll() + sum3.addAll() + sum4.addAll() + residue;
  }

  public static double add(double x, double y) {
    return x + y;
  }
}
