package com.openkappa.splitmap;

import java.util.EnumMap;

public class QueryContext<Value, FilterModel extends Enum<FilterModel> & Filter<Value>,  MetricModel extends Enum<MetricModel> & Metric<Value>> {

  private final EnumMap<FilterModel, SplitMap> filters;
  private final EnumMap<MetricModel, PrefixIndex<ChunkedDoubleArray>> metrics;

  QueryContext(EnumMap<FilterModel, SplitMap> filters, EnumMap<MetricModel, PrefixIndex<ChunkedDoubleArray>> metrics) {
    this.filters = filters;
    this.metrics = metrics;
  }

  public SplitMap getSplitMap(FilterModel filter) {
    return filters.get(filter);
  }

  public PrefixIndex<ChunkedDoubleArray> getMetricColumn(MetricModel metric) {
    return metrics.get(metric);
  }

}
