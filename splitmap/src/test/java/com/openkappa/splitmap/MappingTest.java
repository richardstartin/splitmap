package com.openkappa.splitmap;

import com.openkappa.splitmap.models.Average;
import com.openkappa.splitmap.models.Sum;
import com.openkappa.splitmap.models.SumProduct;
import com.openkappa.splitmap.models.VerticalSum;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.ToDoubleFunction;
import java.util.stream.IntStream;

import static com.openkappa.splitmap.MappingTest.MyMetrics.PRICE;
import static com.openkappa.splitmap.MappingTest.MyMetrics.QUANTITY;

public class MappingTest {


  private static final String[] names = new String[]{"foo", "bar", "blort"};

  private static MyDomainObject randomDomainObject() {
    return new MyDomainObject(names[ThreadLocalRandom.current().nextInt(names.length)],
            LocalDate.now(), ThreadLocalRandom.current().nextDouble(1E9),
            ThreadLocalRandom.current().nextDouble(1E6));
  }

  @Test
  public void mappingPOC() {
    Mapper<MyDomainObject, String, MyMetrics> mapper = Mapper.<MyDomainObject, String, MyMetrics>builder()
            .withFilter("foo", x -> x.getName().equals("foo"))
            .withFilter("bar", x -> x.getName().equals("bar"))
            .withFilter("expensive", x -> x.getPrice() > 10000)
            .withMetricModel(MyMetrics.class).build();
    IntStream.range(0, 100000)
            .mapToObj(i -> randomDomainObject())
            .forEach(mapper::consume);
    QueryContext<String, MyMetrics> df = mapper.snapshot();
    double revenue = Circuits.evaluateIfKeysIntersect(df, slice -> slice.get("foo").and(slice.get("expensive")),
            "foo", "expensive")
            .stream()
            .parallel()
            .mapToDouble(partition -> partition.reduceDouble(SumProduct.<MyMetrics>reducer(df.getMetric(PRICE), df.getMetric(QUANTITY))))
            .sum();

    long count = Circuits.evaluate(df, slice -> slice.get("bar").xor(slice.get("expensive")), "bar", "expensive").getCardinality();

    double avgPrice = Circuits.evaluate(df, slice -> slice.get("bar").andNot(slice.get("expensive")), "bar", "expensive")
            .stream()
            .map(partition -> partition.reduce(Average.<MyMetrics>reducer(df.getMetric(PRICE))))
            .collect(Average.collector());

    double sumQty = Circuits.evaluate(df, slice -> slice.get("foo").or(slice.get("expensive")), "foo", "expensive")
            .stream()
            .parallel()
            .map(partition -> partition.reduce(VerticalSum.reducer(df.getMetric(QUANTITY))))
            .collect(VerticalSum.horizontalSum());

    double sumQty2 = Circuits.evaluate(df, slice -> slice.get("foo").or(slice.get("expensive")), "foo", "expensive")
            .stream()
            .parallel()
            .mapToDouble(partition -> partition.reduceDouble(Sum.reducer(df.getMetric(QUANTITY))))
            .sum();
  }


  enum MyMetrics implements Metric<MyDomainObject> {
    PRICE(MyDomainObject::getPrice),
    QUANTITY(MyDomainObject::getQty);
    private final ToDoubleFunction<MyDomainObject> extractor;

    MyMetrics(ToDoubleFunction<MyDomainObject> extractor) {
      this.extractor = extractor;
    }


    @Override
    public ToDoubleFunction<MyDomainObject> extractor() {
      return extractor;
    }
  }

  private static class MyDomainObject {
    private final String name;
    private final LocalDate date;
    private final double qty;
    private final double price;

    MyDomainObject(String name, LocalDate date, double qty, double price) {
      this.name = name;
      this.date = date;
      this.qty = qty;
      this.price = price;
    }

    public String getName() {
      return name;
    }

    public LocalDate getDate() {
      return date;
    }

    public double getQty() {
      return qty;
    }

    public double getPrice() {
      return price;
    }
  }


  public static void main(String[] args) {
    System.out.println(Integer.toBinaryString(0x55555555));
    System.out.println(Integer.toBinaryString(0x33333333));
    System.out.println(Integer.toBinaryString(0x0F0F0F0F));
    System.out.println(Integer.toBinaryString(0x00FF00FF));
    System.out.println(Integer.toBinaryString(0x0000FFFF));

    System.out.println(Integer.toBinaryString(0x3f));
  }

}
