package com.openkappa.splitmap;

import java.util.EnumMap;
import java.util.Iterator;

public class Slice<Attribute extends Enum<Attribute>, Value> implements Iterable<Value> {

  private final EnumMap<Attribute, Value> values;
  private final Value defaultValue;

  public Slice(Class<Attribute> model, Value defaultValue) {
    this.values = new EnumMap<>(model);
    this.defaultValue = defaultValue;
  }

  public Value get(Attribute field) {
    return values.getOrDefault(field, defaultValue);
  }

  public void set(Attribute field, Value value) {
    values.put(field, value);
  }

  @Override
  public Iterator<Value> iterator() {
    return values.values().iterator();
  }
}
