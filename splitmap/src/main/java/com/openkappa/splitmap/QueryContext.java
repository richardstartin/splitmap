package com.openkappa.splitmap;

import java.util.Arrays;
import java.util.EnumMap;

public class QueryContext<Value, FilterModel extends Enum<FilterModel> & Filter<Value>,  MetricModel extends Enum<MetricModel> & Metric<Value>> {

  private final Class<FilterModel> filterModel;
  private final EnumMap<FilterModel, SplitMap> filters;
  private final EnumMap<MetricModel, PrefixIndex<ChunkedDoubleArray>> metrics;

  QueryContext(Class<FilterModel> filterModel,
               EnumMap<FilterModel, SplitMap> filters,
               EnumMap<MetricModel, PrefixIndex<ChunkedDoubleArray>> metrics) {
    this.filterModel = filterModel;
    this.filters = filters;
    this.metrics = metrics;
  }

  public Class<FilterModel> getFilterModel() {
    return filterModel;
  }

  public SplitMap getSplitMap(FilterModel filter) {
    return filters.get(filter);
  }

  public SplitMap[] indicesFor(FilterModel... filters) {
    return Arrays.stream(filters).map(this.filters::get).toArray(SplitMap[]::new);
  }

  public PrefixIndex<ChunkedDoubleArray> getMetric(MetricModel metric) {
    return metrics.get(metric);
  }

}
