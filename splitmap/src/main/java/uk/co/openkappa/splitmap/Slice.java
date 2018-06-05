package uk.co.openkappa.splitmap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Slice<Attribute, Value> implements Iterable<Value> {

  private final Map<Attribute, Value> values;
  private final Value defaultValue;

  public Slice(Value defaultValue) {
    this.values = new HashMap<>();
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
