package com.openkappa.splitmap;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.*;

import java.util.Arrays;
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

  @Setup(Level.Trial)
  public void setup() {
    splitMaps = IntStream.range(0, count)
            .mapToObj(i -> randomSplitmap(keys, runniness, dirtiness))
            .toArray(SplitMap[]::new);
    indices = Arrays.stream(splitMaps).map(SplitMap::getIndex).toArray(PrefixIndex[]::new);
    bitmaps = IntStream.range(0, count)
            .mapToObj(i -> RoaringBitmap.bitmapOf(randomArray(keys, runniness, dirtiness)))
            .toArray(RoaringBitmap[]::new);
  }


  @Benchmark
  public SplitMap circuit1SplitMap() {
    return Circuits.evaluate(slice -> {
      Container difference = new ArrayContainer();
      Container union = new ArrayContainer();
      for (Container container : slice) {
        if (container.getCardinality() != 0) {
          difference = difference.ixor(container);
          union = union.lazyIOR(container);
        }
      }
      return difference.iand(union);
    }, splitMaps);
  }

  @Benchmark
  public PrefixIndex<?> groupByKey() {
    return Circuits.groupByKey(new ArrayContainer(), indices);
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
