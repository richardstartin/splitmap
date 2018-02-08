package com.openkappa.splitmap.reduction;

import com.openkappa.splitmap.PrefixIndex;
import com.openkappa.splitmap.ReductionContext;

import java.util.function.BinaryOperator;
import java.util.function.DoubleBinaryOperator;

public class DoubleArrayReductionContext<I extends Enum<I>, O extends Enum<O>> implements ReductionContext<I, O, double[]> {

  private final PrefixIndex[] inputs;
  private final double[] output;

  public DoubleArrayReductionContext(int size, PrefixIndex... inputs) {
    this.inputs = inputs;
    this.output = new double[size];
  }

  @Override
  public <U> U readChunk(I column, short key) {
    return readChunk(column.ordinal(), key);
  }

  @Override
  public <U> U readChunk(int column, short key) {
    return (U)inputs[column].get(key);
  }

  @Override
  public void contributeDouble(O column, double value, DoubleBinaryOperator op) {
    output[column.ordinal()] = op.applyAsDouble(output[column.ordinal()], value);
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
