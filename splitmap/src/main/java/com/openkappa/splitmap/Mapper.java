package com.openkappa.splitmap;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.IntUnaryOperator;

public class Mapper<Value, FilterModel extends Enum<FilterModel> & Filter<Value>,  MetricModel extends Enum<MetricModel> & Metric<Value>> {


  public static <Value, FilterModel extends Enum<FilterModel> & Filter<Value>,  MetricModel extends Enum<MetricModel> & Metric<Value>>
  Builder<Value, FilterModel, MetricModel> builder() {
    return new Builder<>();
  }

  private final EnumMap<FilterModel, SplitMapPageWriter> filters;
  private final EnumMap<MetricModel, DoubleArrayPageWriter> metrics;
  private final Class<FilterModel> filterModel;
  private final Class<MetricModel> metricModel;

  private int index = 0;

  Mapper(Class<FilterModel> filterModel, Class<MetricModel> metricModel, IntUnaryOperator hash) {
    this.filters = buildFilters(filterModel, hash);
    this.metrics = buildMetrics(metricModel, hash);
    this.filterModel = filterModel;
    this.metricModel = metricModel;
  }

  public void consume(Value value) {
    filters.entrySet()
           .stream()
           .filter(f -> f.getKey().predicate().test(value))
           .map(Map.Entry::getValue)
           .forEach(w -> w.add(index));
    metrics.forEach((metric, writer) -> writer.add(index, metric.extractor().applyAsDouble(value)));
    ++index;
  }

  public QueryContext<FilterModel, MetricModel> snapshot() {
    return new QueryContext<>(snapshotFilters(filterModel, filters), snapshotMetrics(metricModel, metrics));
  }


  public static class Builder<Value, FilterModel extends Enum<FilterModel> & Filter<Value>,  MetricModel extends Enum<MetricModel> & Metric<Value>> {
    private Class<FilterModel> filterModel;
    private Class<MetricModel> metricModel;
    private IntUnaryOperator hash = InvertibleHashing::scatter;


    public Builder<Value, FilterModel, MetricModel> withFilterModel(Class<FilterModel> filterModel) {
      this.filterModel = filterModel;
      return this;
    }

    public Builder<Value, FilterModel, MetricModel> withMetricModel(Class<MetricModel> metricModel) {
      this.metricModel = metricModel;
      return this;
    }

    public Builder<Value, FilterModel, MetricModel> setHash(IntUnaryOperator hash) {
      this.hash = hash;
      return this;
    }

    public Mapper<Value, FilterModel, MetricModel> build() {
      if (null == filterModel) {
        throw new IllegalStateException("Must provide filter model");
      }
      if (null == metricModel) {
        throw new IllegalStateException("Must provide metric model");
      }
      return new Mapper<>(filterModel, metricModel, hash);
    }
  }

  private static <Value, FilterModel extends Enum<FilterModel> & Filter<Value>>
  EnumMap<FilterModel, SplitMap> snapshotFilters(Class<FilterModel> filterModel,
                                                 EnumMap<FilterModel, SplitMapPageWriter> state) {
    EnumMap<FilterModel, SplitMap> filters = new EnumMap<>(filterModel);
    state.forEach((filter, writer) -> filters.put(filter, writer.toSplitMap()));
    return filters;
  }


  private static <Value, MetricModel extends Enum<MetricModel> & Metric<Value>>
  EnumMap<MetricModel, PrefixIndex<ChunkedDoubleArray>> snapshotMetrics(Class<MetricModel> metricModel,
                                                                        EnumMap<MetricModel, DoubleArrayPageWriter> state) {
    EnumMap<MetricModel, PrefixIndex<ChunkedDoubleArray>> metrics = new EnumMap<>(metricModel);
    state.forEach((filter, writer) -> metrics.put(filter, writer.toIndex()));
    return metrics;
  }

  private static <Value, FilterModel extends Enum<FilterModel> & Filter<Value>>
  EnumMap<FilterModel, SplitMapPageWriter> buildFilters(Class<FilterModel> filterModel, IntUnaryOperator hash) {
    EnumMap<FilterModel, SplitMapPageWriter> filters = new EnumMap<>(filterModel);
    for (FilterModel filter : EnumSet.allOf(filterModel)) {
      filters.put(filter, new SplitMapPageWriter(hash));
    }
    return filters;
  }

  private static <Value, MetricModel extends Enum<MetricModel> & Metric<Value>>
  EnumMap<MetricModel, DoubleArrayPageWriter> buildMetrics(Class<MetricModel> metricModel, IntUnaryOperator hash) {
    EnumMap<MetricModel, DoubleArrayPageWriter> metrics = new EnumMap<>(metricModel);
    for (MetricModel metric : EnumSet.allOf(metricModel)) {
      metrics.put(metric, new DoubleArrayPageWriter(hash));
    }
    return metrics;
  }

}
