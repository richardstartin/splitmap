# splitmap

This library builds on top of [RoaringBitmap](https://github.com/RoaringBitmap/RoaringBitmap) to provide a parallel implementation of boolean circuits (multidimensional filters) and arbitrary aggregations over filters.

For instance, to compute a sum product on a dataset filtered such that only one of two conditions holds:
```java
    PrefixIndex<double[]> quantities = ...
    PrefixIndex<double[]> prices = ...
    SplitMap februarySalesIndex = ...
    SplitMap luxuryProductsIndex = ...
    QueryContext<String, PriceQty> context = new QueryContext<>(
    Map.ofEntries(entry("luxuryProducts", luxuryProductsIndex), entry("febSales", februarySalesIndex), 
    Map.ofEntries(entry(PRICE, prices), entry(QTY, quantities)))); 

    double februaryRevenueFromLuxuryProducts = 
            Circuits.evaluateIfKeysIntersect(context, slice -> slice.get("febSales").and(slice.get("luxuryProducts")), "febSales", "luxuryProducts")
            .stream()
            .parallel()
            .mapToDouble(partition -> partition.reduceDouble(SumProduct.<PriceQty>reducer(price, quantities)))
            .sum();
```

Which, over millions of quantities and prices, can be computed in under 200 microseconds on a modern processor, where parallel streams may take upwards of 20ms.

It is easy to write arbitrary routines combining filtering, calculation and aggregation. For example statistical calculations evaluated with filter criteria.

```java
  public double productMomentCorrelationCoefficient() {
    // calculate the correlation coefficient between prices observed on different exchanges
    PrefixIndex<double[]> exchange1Prices = ...
    PrefixIndex<double[]> exchange2Prices = ...
    SplitMap beforeClose = ...
    SplitMap afterOpen = ...
    QueryContext<String, PriceQty> context = new QueryContext<>(
    Map.ofEntries(entry(BEFORE_CLOSE, beforeClose), entry(AFTER_OPEN, afterOpen), 
    Map.ofEntries(entry(NASDAQ, exchange1Prices), entry(LSE, exchange2Prices)))); 
    // evaluate product moment correlation coefficient 
    return Circuits.evaluate(context, slice -> slice.get(BEFORE_CLOSE).or(slice.get(AFTER_OPEN)), 
            BEFORE_CLOSE, AFTER_OPEN) 
            .stream()
            .parallel()
            .map(partition -> partition.reduce(SimpleLinearRegression.<Exchanges>reducer(exchange1Prices, exchange2Prices)))
            .collect(SimpleLinearRegression.pmcc());
  }
```
