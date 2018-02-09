# splitmap
A parallel bitmap implementation

This library builds on top of [RoaringBitmap](https://github.com/RoaringBitmap/RoaringBitmap) to provide a parallel implementation of boolean circuits (multidimensional filters) and arbitrary aggregations over the filtered sets.

For instance, to compute a sum product on a dataset filtered such that only one of two conditions holds:
```java
    PrefixIndex<double[]> quantities = ...
    PrefixIndex<double[]> prices = ...
    SplitMap februarySalesIndex = ...
    SplitMap luxuryProductsIndex = ...

    double februaryRevenueFromLuxuryProducts = 
            Circuits.evaluate(slice -> slice.get(0).and(slice.get(1)), februarySalesIndex, luxuryProductsIndex)
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
    // but only for instrument1 or market1
    SplitMap instrument1Index = ...
    SplitMap market1Index = ...
    // evaluate product moment correlation coefficient 
    return Circuits.evaluate(slice -> slice.get(0).or(slice.get(1)), market1Index,instrument1Index) 
            .stream()
            .parallel()
            .map(partition -> partition.reduce(SimpleLinearRegression.<Exchanges>reducer(exchange1Prices, exchange2Prices)))
            .collect(SimpleLinearRegression.pmcc());
  }
```
