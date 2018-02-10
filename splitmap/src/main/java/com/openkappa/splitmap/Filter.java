package com.openkappa.splitmap;

import java.util.function.Predicate;

public interface Filter<T> {
  Predicate<T> predicate();
}
