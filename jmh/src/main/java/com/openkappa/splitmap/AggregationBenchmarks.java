package com.openkappa.splitmap;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
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
  @Param({"5", "10"})
  private int ccyCount;

  private PrefixIndex<double[]> qty;
  private PrefixIndex<double[]> price;
  private SplitMap[] instrumentIndex;
  private SplitMap[] ccyIndex;
  private List<Trade> trades;
  private String[] instrumentNames;
  private String[] currencies;
  private int instId1;
  private int ccyId;

  @Setup(Level.Trial)
  public void setup() {
    generateRandomTrades();
    indexTrades();
    instId1 = ThreadLocalRandom.current().nextInt(instrumentCount);
    ccyId = ThreadLocalRandom.current().nextInt(ccyCount);
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
            .filter(trade -> trade.instrumentId.equals(instrumentNames[instId1]))
            .mapToDouble(trade -> trade.qty * trade.price)
            .sum();
  }


  @Benchmark
  public double qtyXPriceForInstrumentIndexXOR() {

    return Circuits.evaluate(slice -> slice.get(0).xor(slice.get(1)),
            instrumentIndex[instId1], ccyIndex[ccyId])
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
  public double qtyXPriceForInstrumentStreamXOR() {
    return trades.parallelStream()
            .filter(trade ->
                    ((trade.instrumentId.equals(instrumentNames[instId1]) && !trade.ccyId.equals(currencies[ccyId]))
                    || (!trade.instrumentId.equals(instrumentNames[instId1]) && trade.ccyId.equals(currencies[ccyId]))))
            .mapToDouble(trade -> trade.qty * trade.price)
            .sum();
  }


  private void indexTrades() {
    PageWriter[] instrumentWriters = IntStream.range(0, instrumentCount)
            .mapToObj(i -> new PageWriter())
            .toArray(PageWriter[]::new);
    PageWriter[] ccyWriters = IntStream.range(0, ccyCount)
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
      instrumentWriters[Arrays.binarySearch(instrumentNames, trade.instrumentId)].add(index);
      ccyWriters[Arrays.binarySearch(currencies, trade.ccyId)].add(index);
      qtyPage[index & 0xFFFF] = trade.qty;
      pricePage[index & 0xFFFF] = trade.price;
      ++index;
    }
    qty.insert((short)(index >>> 16), Arrays.copyOf(qtyPage, qtyPage.length));
    price.insert((short)(index >>> 16), Arrays.copyOf(pricePage, pricePage.length));
    instrumentIndex = Arrays.stream(instrumentWriters).map(PageWriter::toSplitMap).toArray(SplitMap[]::new);
    ccyIndex = Arrays.stream(ccyWriters).map(PageWriter::toSplitMap).toArray(SplitMap[]::new);
  }

  private void generateRandomTrades() {
    currencies = IntStream.range(0, ccyCount)
            .mapToObj(i -> UUID.randomUUID().toString())
            .sorted()
            .toArray(String[]::new);
    instrumentNames = IntStream.range(0, instrumentCount)
            .mapToObj(i -> UUID.randomUUID().toString())
            .sorted()
            .toArray(String[]::new);
    trades = IntStream.range(0, tradeCount)
            .mapToObj(i -> new Trade(ThreadLocalRandom.current().nextDouble(),
                    ThreadLocalRandom.current().nextDouble(),
                    instrumentNames[ThreadLocalRandom.current().nextInt(instrumentCount)],
                    currencies[ThreadLocalRandom.current().nextInt(ccyCount)],
                    UUID.randomUUID().toString()))
            .collect(Collectors.toList());
  }




  private static class Trade {
    private final double price;
    private final double qty;
    private final String instrumentId;
    private final String ccyId;
    private final String tradeId;

    private Trade(double price, double qty, String instrumentId, String ccyId, String tradeId) {
      this.price = price;
      this.qty = qty;
      this.instrumentId = instrumentId;
      this.ccyId = ccyId;
      this.tradeId = tradeId;
    }

    public double getPrice() {
      return price;
    }

    public double getQty() {
      return qty;
    }

    public String getInstrumentId() {
      return instrumentId;
    }

    public String getCcyId() {
      return ccyId;
    }
  }
}
