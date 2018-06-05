package uk.co.openkappa.splitmap;

import java.util.function.ToDoubleFunction;

public interface Metric<T> {
  ToDoubleFunction<T> extractor();
}
