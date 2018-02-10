package com.openkappa.splitmap;

import com.openkappa.splitmap.models.SumProduct;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.stream.IntStream;

import static com.openkappa.splitmap.MappingTest.MyFilters.EXPENSIVE;
import static com.openkappa.splitmap.MappingTest.MyFilters.FOO_FILTER;
import static com.openkappa.splitmap.MappingTest.MyMetrics.PRICE;
import static com.openkappa.splitmap.MappingTest.MyMetrics.QUANTITY;

public class MappingTest {


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

  /* This definitely doesn't scale at all - come up with a better way */
  enum MyFilters implements Filter<MyDomainObject> {

    FOO_FILTER(mdo -> mdo.getName().equals("foo")),
    BAR_FILTER(mdo -> mdo.getName().equals("bar")),
    EXPENSIVE(mdo -> mdo.getPrice() > 10000);

    @Override
    public Predicate<MyDomainObject> predicate() {
      return predicate;
    }

    private final Predicate<MyDomainObject> predicate;

    MyFilters(Predicate<MyDomainObject> predicate) {
      this.predicate = predicate;
    }
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

  @Test
  public void mappingPOC() {
    Mapper<MyDomainObject, MyFilters, MyMetrics> mapper = Mapper.<MyDomainObject, MyFilters, MyMetrics>builder()
            .setFilterModel(MyFilters.class).setMetricModel(MyMetrics.class).build();
    IntStream.range(0, 100000)
            .mapToObj(i -> randomDomainObject())
            .forEach(mapper::consume);
    QueryContext<MyDomainObject, MyFilters, MyMetrics> ctx = mapper.snapshot();
    double revenue = Circuits.evaluateIfKeysIntersect(slice -> slice.get(0).and(slice.get(1)),
            ctx.getSplitMap(FOO_FILTER), ctx.getSplitMap(EXPENSIVE))
            .stream()
            .parallel()
            .mapToDouble(partition -> partition.reduceDouble(SumProduct.<MyMetrics>reducer(ctx.getMetricColumn(PRICE), ctx.getMetricColumn(QUANTITY))))
            .sum();
  }


  private static MyDomainObject randomDomainObject() {
    return new MyDomainObject(names[ThreadLocalRandom.current().nextInt(names.length)],
            LocalDate.now(), ThreadLocalRandom.current().nextDouble(1E9),
            ThreadLocalRandom.current().nextDouble(1E6));
  }


  private static final String[] names = new String[]{"foo", "bar", "blort"};
}
