package com.openkappa.splitmap;

import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.Container;

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
    instId1 = ThreadLocalRandom.current().nextInt(instrumentCount);
    ccyId = ThreadLocalRandom.current().nextInt(ccyCount);
    generateRandomTrades();
    indexTrades();
  }

  @Benchmark
  public double qtyXPriceForInstrumentIndex() {
    return instrumentIndex[instId1]
            .getIndex()
            .streamUniformPartitions()
            .parallel()
            .mapToDouble(partition -> {
              double[] closure = new double[1];
              partition.forEach((k, c) -> {
                double[] l = qty.get(k);
                double[] r = price.get(k);
                c.forEach(k, i -> closure[0] += l[i & 0xFFFF] * r[i & 0xFFFF]);
              });
              return closure[0];
            }).sum();
  }


  @Benchmark
  public double productMomentCorrelationCoefficient() {
    double[] factors =
            Circuits.evaluate(slice -> {
                Container result = slice.get(0).and(slice.get(1));
                return result.getCardinality() == 0 ? null : result;
            }, instrumentIndex[instId1], ccyIndex[ccyId])
            .getIndex()
            .streamUniformPartitions()
            .parallel()
            .map(partition -> {
              double[] stats = new double[6];
              partition.forEach((k, c) -> {
                double[] q = qty.get(k);
                double[] p = price.get(k);
                //Container c = slice.get(0).xor(slice.get(1));
                c.forEach((short)0, i -> {
                  double sq = q[i];
                  double sp = p[i];
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
            }).reduce(new double[6], (x, y) -> {
              for (int i = 0; i < x.length; ++i) {
                x[i] += y[i];
              }
              return x;
            });
    double sq = factors[0];
    double sp = factors[1];
    double spp = factors[2];
    double sqq = factors[3];
    double spq = factors[4];
    double n = factors[5];
    return (n * spq - sq * sp) / (Math.sqrt((n * spp - sp * sp) * (n * sqq - sq * sq)));
  }


  @Benchmark
  public double qtyXPriceForInstrumentStream() {
    return trades.parallelStream()
            .filter(trade -> trade.instrumentName.equals(instrumentNames[instId1]))
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
              double[] closure = new double[1];
              partition.forEach((k, c) -> {
                double[] l = qty.get(k);
                double[] r = price.get(k);
                c.forEach((short)0, i -> closure[0] += l[i] * r[i]);
              });
              return closure[0];
            }).sum();
  }


  @Benchmark
  public double qtyXPriceForInstrumentStreamXOR() {
    return trades.parallelStream()
            .filter(trade ->
                    ((trade.instrumentName.equals(instrumentNames[instId1]) && !trade.ccyId.equals(currencies[ccyId]))
                            || (!trade.instrumentName.equals(instrumentNames[instId1]) && trade.ccyId.equals(currencies[ccyId]))))
            .mapToDouble(trade -> trade.qty * trade.price)
            .sum();
  }


  private void indexTrades() {
    PageWriter[] instrumentWriters = IntStream.range(0, instrumentCount)
            .mapToObj(i -> new PageWriter(Hashing::scatter))
            .toArray(PageWriter[]::new);
    PageWriter[] ccyWriters = IntStream.range(0, ccyCount)
            .mapToObj(i -> new PageWriter(Hashing::scatter))
            .toArray(PageWriter[]::new);
    double[] qtyPage = new double[1 << 16];
    double[] pricePage = new double[1 << 16];

    qty = new PrefixIndex<>();
    price = new PrefixIndex<>();
    short index = 0;
    int x = 0;
    for (Trade trade : trades) {
      if (index == -1) {
        short key = (short)Hashing.scatter((short)(x >>> 16));
        qty.insert(key, Arrays.copyOf(qtyPage, qtyPage.length));
        price.insert(key, Arrays.copyOf(pricePage, pricePage.length));
      }
      int instrumentIndex = trade.instrumentId;
      int ccyIndex = Arrays.binarySearch(currencies, trade.ccyId);
      instrumentWriters[instrumentIndex].add(x);
      ccyWriters[ccyIndex].add(x);
      qtyPage[index & 0xFFFF] = trade.qty;
      pricePage[index & 0xFFFF] = trade.price;
      ++index;
      ++x;
    }
    short key = (short)Hashing.scatter((short)(x >>> 16));
    qty.insert(key, Arrays.copyOf(qtyPage, qtyPage.length));
    price.insert(key, Arrays.copyOf(pricePage, pricePage.length));
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
            .mapToObj(i -> {
              int instrumentId = ThreadLocalRandom.current().nextInt(instrumentCount);
              return new Trade(ThreadLocalRandom.current().nextDouble(),
                    ThreadLocalRandom.current().nextDouble(),
                      instrumentNames[instrumentId],
                    instrumentId,
                    currencies[ThreadLocalRandom.current().nextInt(ccyCount)],
                    UUID.randomUUID().toString());})
            .collect(Collectors.toList());
  }


  private static class Trade {
    private final double price;
    private final double qty;
    private final String instrumentName;
    private final String ccyId;
    private final String tradeId;
    private final int instrumentId;

    private Trade(double price,
                  double qty,
                  String instrumentName,
                  int instrumentId,
                  String ccyId,
                  String tradeId) {
      this.price = price;
      this.qty = qty;
      this.instrumentId = instrumentId;
      this.instrumentName = instrumentName;
      this.ccyId = ccyId;
      this.tradeId = tradeId;
    }

    public double getPrice() {
      return price;
    }

    public double getQty() {
      return qty;
    }

    public String getInstrumentName() {
      return instrumentName;
    }

    public String getCcyId() {
      return ccyId;
    }

    public int getInstrumentId() {
      return instrumentId;
    }
  }
}
