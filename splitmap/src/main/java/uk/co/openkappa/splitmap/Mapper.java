package uk.co.openkappa.splitmap;

import java.util.*;
import java.util.function.Predicate;

public class Mapper<Value, FilterModel, MetricModel extends Enum<MetricModel> & Metric<Value>> {


  private final Map<FilterModel, Predicate<Value>> filters;
  private final Map<FilterModel, SplitMapPageWriter> filterWriters;
  private final EnumMap<MetricModel, DoubleArrayPageWriter> metrics;
  private final Class<MetricModel> metricModel;
  private int index = 0;

  Mapper(Map<FilterModel, Predicate<Value>> filters, Class<MetricModel> metricModel, KeyInvolution involution) {
    this.filters = filters;
    this.filterWriters = buildFilters(filters.keySet(), involution);
    this.metrics = buildMetrics(metricModel, involution);
    this.metricModel = metricModel;
  }

  public static <Value, FilterModel, MetricModel extends Enum<MetricModel> & Metric<Value>>
  Builder<Value, FilterModel, MetricModel> builder() {
    return new Builder<>();
  }

  private static <FilterModel>
  Map<FilterModel, SplitMap> snapshotFilters(Map<FilterModel, SplitMapPageWriter> state) {
    Map<FilterModel, SplitMap> filters = new HashMap<>();
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

  private static <FilterModel>
  Map<FilterModel, SplitMapPageWriter> buildFilters(Set<FilterModel> filters, KeyInvolution involution) {
    Map<FilterModel, SplitMapPageWriter> filterWriters = new HashMap<>();
    for (FilterModel filter : filters) {
      filterWriters.put(filter, new SplitMapPageWriter(involution));
    }
    return filterWriters;
  }

  private static <Value, MetricModel extends Enum<MetricModel> & Metric<Value>>
  EnumMap<MetricModel, DoubleArrayPageWriter> buildMetrics(Class<MetricModel> metricModel, KeyInvolution involution) {
    EnumMap<MetricModel, DoubleArrayPageWriter> metrics = new EnumMap<>(metricModel);
    for (MetricModel metric : EnumSet.allOf(metricModel)) {
      metrics.put(metric, new DoubleArrayPageWriter(involution));
    }
    return metrics;
  }

  public void consume(Value value) {
    filters.entrySet()
            .stream()
            .filter(f -> f.getValue().test(value))
            .map(f -> filterWriters.get(f.getKey()))
            .forEach(w -> w.add(index));
    metrics.forEach((metric, writer) -> writer.add(index, metric.extractor().applyAsDouble(value)));
    ++index;
  }

  public QueryContext<FilterModel, MetricModel> snapshot() {
    return new QueryContext<>(snapshotFilters(filterWriters), snapshotMetrics(metricModel, metrics));
  }

  public static class Builder<Value, FilterModel, MetricModel extends Enum<MetricModel> & Metric<Value>> {
    private Map<FilterModel, Predicate<Value>> filters = new HashMap<>();
    private Class<MetricModel> metricModel;
    private KeyInvolution involution = Involutions::reverse;


    public Builder<Value, FilterModel, MetricModel> withFilter(FilterModel field, Predicate<Value> predicate) {
      filters.put(field, predicate);
      return this;
    }

    public Builder<Value, FilterModel, MetricModel> withMetricModel(Class<MetricModel> metricModel) {
      this.metricModel = metricModel;
      return this;
    }

    public Builder<Value, FilterModel, MetricModel> withKeyInvolution(KeyInvolution hash) {
      this.involution = involution;
      return this;
    }

    public Mapper<Value, FilterModel, MetricModel> build() {
      if (null == metricModel) {
        throw new IllegalStateException("Must provide metric model");
      }
      return new Mapper<>(filters, metricModel, involution);
    }
  }

}
