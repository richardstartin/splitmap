package com.openkappa.splitmap;

@FunctionalInterface
public interface ToDoubleBiFunction<T, U> {
  double applyAsDouble(T t, U u);
}
