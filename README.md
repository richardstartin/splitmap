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
            .streamBalancedPartitions()
            .parallel()
            .mapToDouble(partition -> {
              double[] sumProduct =  new double[1];
              partition.forEach((key, mask) -> {
                double[] q = quantities.get(key);
                double[] p = prices.get(key);
                mask.forEach(key, i -> sumProduct[0] += q[i & 0xFFFF] * p[i & 0xFFFF]);
              });
              return sumProduct[0];
            }).sum();
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
    double[] factors = Circuits.evaluate(slice -> slice.get(0).or(slice.get(1)) // filter criteria
            .getIndex()
            .streamBalancedPartitions() // allow parallel splitting
            .parallel() // go parallel
            .map(partition -> { // compute stats factors per partition
              double[] stats = new double[6];
              partition.forEach((k, c) -> {
                double[] q = exchange1Prices.get(k);
                double[] p = exchange2Prices.get(k);
                c.forEach(k, i -> {
                  int index = i & 0xFFFF;
                  double sq = q[index];
                  double sp = p[index];
                  double spp = sp * sp;
                  double sqq = sq * sq;
                  double spq = sp * sq;
                  stats[0] += sq;
                  stats[1] += sp;
                  stats[2] += spp;
                  stats[3] += sqq;
                  stats[4] += spq;
                  stats[5] += 1;
                });
              });
              return stats;
            }).reduce(new double[6], (x, y) -> { // reduce
              for (int i = 0; i < x.length; ++i) {
                x[i] += y[i];
              }
              return x;
            });
    // combine the aggregates
    double sq = factors[0];
    double sp = factors[1];
    double spp = factors[2];
    double sqq = factors[3];
    double spq = factors[4];
    double n = factors[5];
    return (n * spq - sq * sp) / (Math.sqrt((n * spp - sp * sp) * (n * sqq - sq * sq)));
  }
```
