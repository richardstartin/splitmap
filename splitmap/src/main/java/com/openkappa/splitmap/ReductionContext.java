package com.openkappa.splitmap;

import java.util.function.BinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.IntBinaryOperator;

public interface ReductionContext<I, O, V> {

  <U> U readChunk(int column, short key);

  default void contributeInt(O column, int value, IntBinaryOperator op) {
    throw new IllegalStateException("Not implemented");
  }

  default void contributeDouble(O column, double value, DoubleBinaryOperator op) {
    throw new IllegalStateException("Not implemented");
  }

  default void contributeLong(O column, long value, DoubleBinaryOperator op) {
    throw new IllegalStateException("Not implemented");
  }

  void contribute(V value, BinaryOperator<V> op);

  V getReducedValue();

  default double getReducedDouble() {
    throw new IllegalStateException("Not implemented");
  }

  default long getReducedLong() {
    throw new IllegalStateException("Not implemented");
  }

  default int getReducedInt() {
    throw new IllegalStateException("Not implemented");
  }
}
