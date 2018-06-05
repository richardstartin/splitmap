package uk.co.openkappa.splitmap;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.Container;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static uk.co.openkappa.splitmap.DataGenerator.randomSplitmap;

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
    Map<Integer, SplitMap> filters = new HashMap<>();
    values = new Integer[count];
    for (int i = 0; i < count; ++i) {
      filters.put(i, splitMaps[i]);
      values[i] = i;

    }
    context = new QueryContext<>(filters, null);
  }


  @Benchmark
  public SplitMap orSplitMap() {
    return Circuits.evaluate(context, slice -> {
      Container union = new BitmapContainer(new long[1024], -1);
      for (Container mask : slice) {
        union = union.lazyIOR(mask);
      }
      return union.repairAfterLazy();
    }, values);
  }

}
