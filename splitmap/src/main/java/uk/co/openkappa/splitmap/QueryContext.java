package uk.co.openkappa.splitmap;

import java.util.Map;

public class QueryContext<FilterModel, MetricModel> {

  private final Map<FilterModel, SplitMap> filters;
  private final Map<MetricModel, PrefixIndex<ChunkedDoubleArray>> metrics;

  QueryContext(Map<FilterModel, SplitMap> filters,
               Map<MetricModel, PrefixIndex<ChunkedDoubleArray>> metrics) {
    this.filters = filters;
    this.metrics = metrics;
  }

  public SplitMap getSplitMap(FilterModel filter) {
    return filters.get(filter);
  }

  public PrefixIndex<ChunkedDoubleArray> getMetric(MetricModel metric) {
    return metrics.get(metric);
  }

}
