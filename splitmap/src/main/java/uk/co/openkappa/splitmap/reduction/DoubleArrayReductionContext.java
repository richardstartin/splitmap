package uk.co.openkappa.splitmap.reduction;

import uk.co.openkappa.splitmap.PrefixIndex;
import uk.co.openkappa.splitmap.ReductionContext;

import java.util.function.BinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.ToIntFunction;

public class DoubleArrayReductionContext<I, O> implements ReductionContext<I, O, double[]> {

  private final PrefixIndex[] inputs;
  private final double[] output;
  private final ToIntFunction<O> outputColumnMapper;

  public DoubleArrayReductionContext(int size, ToIntFunction<O> outputColumnMapper, PrefixIndex... inputs) {
    this.inputs = inputs;
    this.outputColumnMapper = outputColumnMapper;
    this.output = new double[size];
  }

  @Override
  public <U> U readChunk(int column, short key) {
    return (U) inputs[column].get(key);
  }

  @Override
  public void contributeDouble(O column, double value, DoubleBinaryOperator op) {
    contributeDouble(outputColumnMapper.applyAsInt(column), value, op);
  }

  @Override
  public void contributeDouble(int column, double value, DoubleBinaryOperator op) {
    output[column] = op.applyAsDouble(output[column], value);
  }

  @Override
  public void contribute(double[] value, BinaryOperator<double[]> reducer) {
    double[] reduced = reducer.apply(output, value);
    if (reduced != output) {
      System.arraycopy(reduced, 0, output, 0, reduced.length);
    }
  }

  @Override
  public double[] getReducedValue() {
    return output;
  }
}
