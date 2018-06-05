package uk.co.openkappa.splitmap;

import java.util.function.BinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.IntBinaryOperator;

public interface ReductionProcedure<Input, Output, Result, Value>
        extends ReductionContext<Input, Output, Result>, KeyValueConsumer<Value> {

  static <Input, Output, Result, Value>
  ReductionProcedure<Input, Output, Result, Value> mixin(ReductionContext<Input, Output, Result> context,
                                                         KeyValueConsumer<Value> consumer) {
    return new ReductionProcedure<>() {
      @Override
      public void accept(short key, Value value) {
        consumer.accept(key, value);
      }

      @Override
      public <U> U readChunk(int column, short key) {
        return context.readChunk(column, key);
      }

      @Override
      public void contributeInt(Output column, int value, IntBinaryOperator op) {
        context.contributeInt(column, value, op);
      }

      @Override
      public void contributeDouble(Output column, double value, DoubleBinaryOperator op) {
        context.contributeDouble(column, value, op);
      }

      @Override
      public void contributeLong(Output column, long value, DoubleBinaryOperator op) {
        context.contributeLong(column, value, op);
      }

      @Override
      public void contribute(Result value, BinaryOperator<Result> op) {
        context.contribute(value, op);
      }

      @Override
      public Result getReducedValue() {
        return context.getReducedValue();
      }

      @Override
      public double getReducedDouble() {
        return context.getReducedDouble();
      }

      @Override
      public long getReducedLong() {
        return context.getReducedLong();
      }

      @Override
      public int getReducedInt() {
        return context.getReducedInt();
      }
    };
  }


}
