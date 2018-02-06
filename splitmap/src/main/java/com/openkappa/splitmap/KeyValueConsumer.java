package com.openkappa.splitmap;

@FunctionalInterface
public interface KeyValueConsumer<T> {
  void accept(short key, T value);
}
