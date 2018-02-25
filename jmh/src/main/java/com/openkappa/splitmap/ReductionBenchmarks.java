package com.openkappa.splitmap;

import com.openkappa.splitmap.models.*;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Map.entry;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ReductionBenchmarks {

  enum PriceQty {
    PRICE,
    QUANTITY
  }


  @Param({"1000000"/*, "10000000"*/})
  private int tradeCount;
  @Param({"10", /*"100"*/})
  private int instrumentCount;
  @Param({"5", /*"10"*/})
  private int ccyCount;

  private PrefixIndex<ChunkedDoubleArray> qty;
  private PrefixIndex<ChunkedDoubleArray> price;
  private SplitMap[] instrumentIndex;
  private SplitMap[] ccyIndex;
  private List<Trade> trades;
  private String[] instrumentNames;
  private String[] currencies;
  private int instId1;
  private int ccyId;
  private double[] test;
  private QueryContext<Integer, PriceQty> context;

  @Setup(Level.Trial)
  public void setup() {
    instId1 = ThreadLocalRandom.current().nextInt(instrumentCount);
    ccyId = ThreadLocalRandom.current().nextInt(ccyCount);
    generateRandomTrades();
    indexTrades();
    test = new double[tradeCount];
    for (int i = 0; i < tradeCount; ++i) {
      test[i] = ThreadLocalRandom.current().nextDouble();
    }
    context = new QueryContext<>(Map.ofEntries(entry(0, instrumentIndex[instId1]), entry(1, ccyIndex[ccyId])),
            new EnumMap<>(
                    Map.ofEntries(entry(PriceQty.PRICE, price), entry(PriceQty.QUANTITY, qty))));
  }

  @Benchmark
  public double sanityCheck() {
    double sum = 0D;
    for (int i = 0; i < test.length; ++i) {
      sum += test[i];
    }
    return sum;
  }

  @Benchmark
  public double sanityCheckStream() {
    return trades.parallelStream()
            .filter(trade -> trade.instrumentName.equals(instrumentNames[instId1]))
            .filter(trade -> trade.ccyId.equals(currencies[ccyId]))
            .mapToDouble(Trade::getQty)
            .sum();
  }

  @Benchmark
  public double reduceQty() {
    return Circuits.evaluate(context, slice -> slice.get(0).or(slice.get(1)), 0, 1)
            .stream()
            .parallel()
            .mapToDouble(partition -> partition.reduceDouble(Sum.reducer(qty)))
            .sum();
  }

  @Benchmark
  public double reduceQtyVectorised() {
    return Circuits.evaluate(context, slice -> slice.get(0).or(slice.get(1)), 0, 1)
            .stream()
            .parallel()
            .map(partition -> partition.reduce(VerticalSum.reducer(qty)))
            .collect(VerticalSum.horizontalSum());
  }

  @Benchmark
  public double qtyXPriceForInstrumentIndex() {
    return Circuits.evaluate(context, slice -> slice.get(0).or(slice.get(1)), 0, 1)
            .stream()
            .parallel()
            .mapToDouble(partition -> partition.reduceDouble(SumProduct.reducer(price, qty)))
            .sum();
  }


  @Benchmark
  public double averageQtyInstrumentAndCcyIndex() {
    return Circuits.evaluate(context, slice -> slice.get(0).or(slice.get(1)), 0, 1)
            .stream()
            .parallel()
            .map(partition -> partition.reduce(Average.reducer(qty)))
            .collect(Average.collector());
  }


  @Benchmark
  public double productMomentCorrelationCoefficient() {
    return Circuits.evaluate(context, slice -> slice.get(0).or(slice.get(1)), 0, 1)
            .stream()
            .parallel()
            .map(partition -> partition.reduce(SimpleLinearRegression.reducer(price, qty)))
            .collect(SimpleLinearRegression.pmcc());
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
    return Circuits.evaluate(context, slice -> slice.get(0).xor(slice.get(1)), 0, 1)
            .stream()
            .parallel()
            .mapToDouble(partition -> partition.reduceDouble(SumProduct.reducer(price, qty)))
            .sum();
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
    SplitMapPageWriter[] instrumentWriters = IntStream.range(0, instrumentCount)
            .mapToObj(i -> new SplitMapPageWriter(Involutions::reverse))
            .toArray(SplitMapPageWriter[]::new);
    SplitMapPageWriter[] ccyWriters = IntStream.range(0, ccyCount)
            .mapToObj(i -> new SplitMapPageWriter(Involutions::reverse))
            .toArray(SplitMapPageWriter[]::new);
    DoubleArrayPageWriter qtyWriter = new DoubleArrayPageWriter(Involutions::reverse);
    DoubleArrayPageWriter priceWriter = new DoubleArrayPageWriter(Involutions::reverse);

    int x = 0;
    for (Trade trade : trades) {
      int instrumentIndex = trade.instrumentId;
      int ccyIndex = Arrays.binarySearch(currencies, trade.ccyId);
      instrumentWriters[instrumentIndex].add(x);
      ccyWriters[ccyIndex].add(x);
      qtyWriter.add(x, trade.qty);
      priceWriter.add(x, trade.price);
      ++x;
    }

    qty = qtyWriter.toIndex();
    price = priceWriter.toIndex();
    instrumentIndex = Arrays.stream(instrumentWriters).map(SplitMapPageWriter::toSplitMap).toArray(SplitMap[]::new);
    ccyIndex = Arrays.stream(ccyWriters).map(SplitMapPageWriter::toSplitMap).toArray(SplitMap[]::new);
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
