package com.openkappa.splitmap;

@FunctionalInterface
public interface KeyConsumer<T> {
  void accept(short key, T value);
}
