# splitmap
A parallel bitmap implementation

This library builds on top of [RoaringBitmap](https://github.com/RoaringBitmap/RoaringBitmap) to provide a parallel implementation of boolean circuits (multidimensional filters) and arbitrary aggregations over the filtered sets.

For instance, to compute a sum product on a dataset filtered such that only one of two conditions holds:
```
    PrefixIndex<double[]> quantities = ...
    PrefixIndex<double[]> prices = ...
    SplitMap februarySalesIndex = ...
    SplitMap luxuryProductsIndex = ...

    double revenue = Circuits.evaluate(slice -> slice.get(0).xor(slice.get(1)), 
                                       februarySalesIndex, luxuryProductsIndex)
            .getIndex()
            .streamUniformPartitions()
            .parallel()
            .mapToDouble(partition -> {
              ReductionContext<?, SumProduct, Double> ctx = SumProduct.createContext(pi1, pi2);
              partition.forEach(SumProduct.createEvaluation(ctx));
              return ctx.getReducedDouble();
            })
            .sum();
```

Which, over millions of quantities and prices, can be computed in under 1ms on a modern processor, where parallel streams may take upwards of 20ms.

It is easy to write arbitrary routines combining filtering, calculation and aggregation. For example statistical calculations evaluated with filter criteria.

```
  public double productMomentCorrelationCoefficient() {
    // calculate the correlation coefficient between prices observed on different exchanges
    PrefixIndex<double[]> exchange1Prices = ...
    PrefixIndex<double[]> exchange2Prices = ...
    // but only for instrument1 or market1
    SplitMap instrument1Index = ...
    SplitMap market1Index = ...
    // evaluate product moment correlation coefficient 
    return Circuits.evaluate(slice -> slice.get(0).or(slice.get(1)) // filter criteria
            .getIndex()
            .streamUniformPartitions() // allow parallel splitting
            .parallel() // go parallel
            .map(partition -> { // compute stats factors per partition
              partition.forEach((k, c) -> {
                ReductionContext<?, LinearRegression, double[]> ctx = 
                        LinearRegression.createContext(exchange1Prices, exchange2Prices);
                partition.forEach(LinearRegression.createEvaluation(ctx));
                return ctx.getReducedValue();
              });
            })
            .collect(PMCC);
  }
```
