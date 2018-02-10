package com.openkappa.splitmap;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.openkappa.splitmap.DataGenerator.randomArray;
import static com.openkappa.splitmap.DataGenerator.randomSplitmap;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CircuitBenchmarks {

  @Param("0.33")
  double runniness;
  @Param("0.33")
  double dirtiness;
  @Param({"64", "256", "512"})
  int keys;
  @Param({"100", "1000"})
  int count;

  SplitMap[] splitMaps;
  PrefixIndex<Container>[] indices;
  RoaringBitmap[] bitmaps;

  private QueryContext<Integer, ?> context;
  private Integer[] values;

  @Setup(Level.Trial)
  public void setup() {
    splitMaps = IntStream.range(0, count)
            .mapToObj(i -> randomSplitmap(keys, runniness, dirtiness))
            .toArray(SplitMap[]::new);
    indices = Arrays.stream(splitMaps).map(SplitMap::getIndex).toArray(PrefixIndex[]::new);
    bitmaps = IntStream.range(0, count)
            .mapToObj(i -> RoaringBitmap.bitmapOf(randomArray(keys, runniness, dirtiness)))
            .toArray(RoaringBitmap[]::new);
    Map<Integer, SplitMap> filters = new HashMap<>();
    values = new Integer[count];
    for (int i = 0; i < count; ++i) {
      filters.put(i, splitMaps[i]);
      values[i] = i;

    }
    context = new QueryContext<>(filters, null);

  }


  @Benchmark
  public SplitMap circuit1SplitMap() {
    return Circuits.evaluate(context, slice -> {
      Container difference = new ArrayContainer();
      Container union = new ArrayContainer();
      for (Container container : slice) {
        if (container.getCardinality() != 0) {
          difference = difference.ixor(container);
          union = union.lazyIOR(container);
        }
      }
      return difference.iand(union);
    }, values);
  }

  @Benchmark
  public RoaringBitmap circuit1Roaring() {
//    return ParallelAggregation.evaluate(slice -> {
//              Container difference = new ArrayContainer();
//              Container union = new ArrayContainer();
//              for (Container container : slice) {
//                if (container.getCardinality() != 0) {
//                  difference = difference.ixor(container);
//                  union = union.lazyIOR(container);
//                }
//              }
//              return difference.iand(union);
//            }, bitmaps);
    return RoaringBitmap.and(FastAggregation.or(bitmaps), FastAggregation.xor(bitmaps));
  }
}
