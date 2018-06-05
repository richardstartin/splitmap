package uk.co.openkappa.splitmap;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static uk.co.openkappa.splitmap.DataGenerator.randomArray;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PointBenchmarks {

  @Param("0.33")
  double runniness;
  @Param("0.33")
  double dirtiness;
  @Param({"64", "256", "512"})
  int keys;

  private SplitMap splitMap;

  private int goodValue;
  private int badValue;

  @Setup(Level.Trial)
  public void setup() {
    int[] data = randomArray(keys, runniness, dirtiness);
    SplitMapPageWriter writer = new SplitMapPageWriter();
    IntStream.of(data).forEach(writer::add);
    splitMap = writer.toSplitMap();
    goodValue = data[ThreadLocalRandom.current().nextInt(data.length)];
    badValue = goodValue;
    while (Arrays.binarySearch(data, badValue) >= 0) {
      badValue = ThreadLocalRandom.current().nextInt();
    }
  }

  @Benchmark
  public long getCardinalitySplitmap() {
    return splitMap.getCardinality();
  }


  @Benchmark
  public boolean containsSplitmap() {
    return splitMap.contains(goodValue);
  }


  @Benchmark
  public boolean containsMissingSplitmap() {
    return splitMap.contains(badValue);
  }
}
