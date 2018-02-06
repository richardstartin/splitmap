package com.openkappa.splitmap;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class AggregationBenchmarks {


  @Param({"1000000", "10000000"})
  private int tradeCount;
  @Param({"10", "100"})
  private int instrumentCount;


  private PrefixIndex<double[]> qty;
  private PrefixIndex<double[]> price;
  private SplitMap[] instrumentIndex;
  private List<Trade> trades;
  private int instId1;
  private int instId2;

  @Setup(Level.Trial)
  public void setup() {
    generateRandomTrades();
    indexTrades();
    instId1 = ThreadLocalRandom.current().nextInt(instrumentCount);
    do {
      instId2 = ThreadLocalRandom.current().nextInt(instrumentCount);
    } while (instId2 == instId1);
  }

  @Benchmark
  public double qtyXPriceForInstrumentIndex() {
    return instrumentIndex[instId1]
            .getIndex()
            .streamUniformPartitions()
            .parallel()
            .mapToDouble(partition -> {
              double[] closure =  new double[1];
              partition.forEach((k, c) -> {
                double[] l = qty.get(k);
                double[] r = price.get(k);
                c.forEach(k, i -> closure[0] += l[i & 0xFFFF] * r[i & 0xFFFF]);
              });
              return closure[0];
            }).sum();

  }


  @Benchmark
  public double qtyXPriceForInstrumentStream() {
    return trades.parallelStream()
            .filter(trade -> trade.instrumentId == instId1)
            .mapToDouble(trade -> trade.qty * trade.price)
            .sum();
  }


  @Benchmark
  public double qtyXPriceForInstrumentIndexOR() {

    return Circuits.evaluate(slice -> slice.get(0).or(slice.get(1)),
            instrumentIndex[instId1], instrumentIndex[instId2])
            .getIndex()
            .streamUniformPartitions()
            .parallel()
            .mapToDouble(partition -> {
              double[] closure =  new double[1];
              partition.forEach((k, c) -> {
                double[] l = qty.get(k);
                double[] r = price.get(k);
                c.forEach(k, i -> closure[0] += l[i & 0xFFFF] * r[i & 0xFFFF]);
              });
              return closure[0];
            }).sum();
  }


  @Benchmark
  public double qtyXPriceForInstrumentStreamOR() {
    return trades.parallelStream()
            .filter(trade -> trade.instrumentId == instId1 || trade.instrumentId == instId2)
            .mapToDouble(trade -> trade.qty * trade.price)
            .sum();
  }


  private void indexTrades() {
    PageWriter[] writers = IntStream.range(0, instrumentCount)
            .mapToObj(i -> new PageWriter())
            .toArray(PageWriter[]::new);
    double[] qtyPage = new double[1 << 16];
    double[] pricePage = new double[1 << 16];
    qty = new PrefixIndex<>();
    price = new PrefixIndex<>();
    int index = 0;
    for (Trade trade : trades) {
      if (index != 0 && (index % 65536) == 0) {
        qty.insert((short)((index >>> 16) - 1), Arrays.copyOf(qtyPage, qtyPage.length));
        price.insert((short)((index >>> 16) - 1), Arrays.copyOf(pricePage, pricePage.length));
      }
      writers[trade.instrumentId].add(index);
      qtyPage[index & 0xFFFF] = trade.qty;
      pricePage[index & 0xFFFF] = trade.price;
      ++index;
    }
    qty.insert((short)(index >>> 16), Arrays.copyOf(qtyPage, qtyPage.length));
    price.insert((short)(index >>> 16), Arrays.copyOf(pricePage, pricePage.length));
    instrumentIndex = Arrays.stream(writers).map(PageWriter::toSplitMap).toArray(SplitMap[]::new);
  }

  private void generateRandomTrades() {
    trades = IntStream.range(0, tradeCount)
            .mapToObj(i -> new Trade(ThreadLocalRandom.current().nextDouble(),
                    ThreadLocalRandom.current().nextDouble(),
                    ThreadLocalRandom.current().nextInt(instrumentCount)
                    )).collect(Collectors.toList());
  }




  private static class Trade {
    private final double price;
    private final double qty;
    private final int instrumentId;

    private Trade(double price, double qty, int instrumentId) {
      this.price = price;
      this.qty = qty;
      this.instrumentId = instrumentId;
    }

    public double getPrice() {
      return price;
    }

    public double getQty() {
      return qty;
    }

    public int getInstrumentId() {
      return instrumentId;
    }
  }
}
