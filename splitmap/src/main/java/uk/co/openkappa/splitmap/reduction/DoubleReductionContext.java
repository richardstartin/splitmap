package uk.co.openkappa.splitmap.reduction;

import uk.co.openkappa.splitmap.PrefixIndex;
import uk.co.openkappa.splitmap.ReductionContext;

import java.util.function.BinaryOperator;
import java.util.function.DoubleBinaryOperator;

public class DoubleReductionContext<I, O> implements ReductionContext<I, O, Double> {

  private final PrefixIndex[] inputs;

  private double output;

  public DoubleReductionContext(PrefixIndex... inputs) {
    this.inputs = inputs;
  }

  @Override
  public <U> U readChunk(int column, short key) {
    return (U) inputs[column].get(key);
  }

  @Override
  public void contributeDouble(O column, double value, DoubleBinaryOperator op) {
    output = op.applyAsDouble(output, value);
  }

  @Override
  public void contribute(Double value, BinaryOperator<Double> op) {
    output = op.apply(output, value);
  }

  @Override
  public Double getReducedValue() {
    return output;
  }

  @Override
  public double getReducedDouble() {
    return output;
  }
}
