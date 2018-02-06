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

Which, over millions of quantities and prices, can be computed in under 1ms on a modern processor, where plain parallel streams may take upwards of 20ms.
