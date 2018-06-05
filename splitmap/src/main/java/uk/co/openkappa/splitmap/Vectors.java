package uk.co.openkappa.splitmap;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.Shapes;
import jdk.incubator.vector.Vector;

public class Vectors {

  public static final DoubleVector.DoubleSpecies<Shapes.S256Bit> D256 = (DoubleVector.DoubleSpecies<Shapes.S256Bit>)
          Vector.species(double.class, Shapes.S_256_BIT);
}