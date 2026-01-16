package dk.alexandra.fresco.bgw;

import dk.alexandra.fresco.bgw.gates.BGWAdditionKnownProtocol;
import dk.alexandra.fresco.bgw.gates.BGWAdditionProtocol;
import dk.alexandra.fresco.bgw.gates.BGWConstantMultiplicationProtocol;
import dk.alexandra.fresco.bgw.gates.BGWInputProtocol;
import dk.alexandra.fresco.bgw.gates.BGWKnownProtocol;
import dk.alexandra.fresco.bgw.gates.BGWMultiplicationProtocol;
import dk.alexandra.fresco.bgw.gates.BGWOutputToAllProtocol;
import dk.alexandra.fresco.bgw.gates.BGWOutputToPartyProtocol;
import dk.alexandra.fresco.bgw.gates.BGWRandomProtocol;
import dk.alexandra.fresco.bgw.gates.BGWSubtractionKnownLeftProtocol;
import dk.alexandra.fresco.bgw.gates.BGWSubtractionKnownRightProtocol;
import dk.alexandra.fresco.bgw.gates.BGWSubtractionProtocol;
import dk.alexandra.fresco.framework.DRes;
import dk.alexandra.fresco.framework.builder.Computation;
import dk.alexandra.fresco.framework.builder.numeric.BuilderFactoryNumeric;
import dk.alexandra.fresco.framework.builder.numeric.Numeric;
import dk.alexandra.fresco.framework.builder.numeric.ProtocolBuilderNumeric;
import dk.alexandra.fresco.framework.value.SInt;
import dk.alexandra.fresco.lib.field.integer.BasicNumericContext;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BGWBuilder implements BuilderFactoryNumeric {

  private final BasicNumericContext context;
  private final int t;
  private final Random random;

  public BGWBuilder(BasicNumericContext context, int t, Random random) {
    this.context = context;
    this.t = t;
    this.random = random;
  }

  public BasicNumericContext getBasicNumericContext() {
    return context;
  }

  public Numeric createNumeric(ProtocolBuilderNumeric builder) {
    return new Numeric() {

      @Override
      public DRes<SInt> add(DRes<SInt> a, DRes<SInt> b) {
        return builder.append(new BGWAdditionProtocol(a, b));
      }

      @Override
      public DRes<SInt> add(BigInteger a, DRes<SInt> b) {
        return builder.append(new BGWAdditionKnownProtocol(a, b));
      }

      @Override
      public DRes<SInt> sub(DRes<SInt> a, DRes<SInt> b) {
        return builder.append(new BGWSubtractionProtocol(a, b));
      }

      @Override
      public DRes<SInt> sub(BigInteger a, DRes<SInt> b) {
        return builder.append(new BGWSubtractionKnownLeftProtocol(a, b));
      }

      @Override
      public DRes<SInt> sub(DRes<SInt> a, BigInteger b) {
        return builder.append(new BGWSubtractionKnownRightProtocol(a, b));
      }

      @Override
      public DRes<SInt> mult(DRes<SInt> a, DRes<SInt> b) {
        return builder.append(new BGWMultiplicationProtocol(a, b));
      }

      @Override
      public DRes<SInt> mult(BigInteger a, DRes<SInt> b) {
        return builder.append(new BGWConstantMultiplicationProtocol(a, b));
      }

      @Override // TODO: Currently log n depth of multiplications?
      public DRes<SInt> randomBit() {
        return builder.par(par -> {
          List<DRes<SInt>> bits = new ArrayList<>();
          for (int i = 1; i <= par.getBasicNumericContext().getNoOfParties(); i++) {
            if (i == par.getBasicNumericContext().getMyId()) {
              bits.add(par.numeric().input(random.nextInt(2) * 2 - 1, i));
            } else {
              bits.add(par.numeric().input(null, i));
            }
          }
          return DRes.of(bits);
        }).seq((seq, bits) -> seq.seq(new ProductSIntList(bits))).seq((seq, product) -> seq.numeric().mult(
            BigInteger.TWO.modInverse(seq.getBasicNumericContext().getModulus()),
            seq.numeric().add(1, product)));
       }

      @Override
      public DRes<SInt> randomElement() {
        return builder.append(new BGWRandomProtocol());
      }

      @Override
      public DRes<SInt> known(BigInteger value) {
        return builder.append(new BGWKnownProtocol(value));
      }

      @Override
      public DRes<SInt> input(BigInteger value, int inputParty) {
        return builder.append(new BGWInputProtocol(value, inputParty));
      }

      @Override
      public DRes<BigInteger> open(DRes<SInt> secretShare) {
        return builder.append(new BGWOutputToAllProtocol(secretShare));
      }

      @Override
      public DRes<BigInteger> open(DRes<SInt> secretShare, int outputParty) {
        return builder.append(new BGWOutputToPartyProtocol(secretShare, outputParty));
      }
    };
  }


  private static class ProductSIntList implements Computation<SInt, ProtocolBuilderNumeric> {

    private final List<DRes<SInt>> input;

    /**
     * Creates a new ProductSIntList.
     *
     * @param list the list to sum
     */
    public ProductSIntList(List<DRes<SInt>> list) {
      input = list;
    }

    @Override
    public DRes<SInt> buildComputation(ProtocolBuilderNumeric iterationBuilder) {
      return iterationBuilder.seq(seq ->
          () -> input
      ).whileLoop(
          (inputs) -> inputs.size() > 1,
          (seq, inputs) -> seq.par(parallel -> {
            List<DRes<SInt>> out = new ArrayList<>();
            Numeric numericBuilder = parallel.numeric();
            DRes<SInt> left = null;
            for (DRes<SInt> input1 : inputs) {
              if (left == null) {
                left = input1;
              } else {
                out.add(numericBuilder.mult(left, input1));
                left = null;
              }
            }
            if (left != null) {
              out.add(left);
            }
            return () -> out;
          })
      ).seq((builder, currentInput) -> currentInput.get(0));
    }
  }
}