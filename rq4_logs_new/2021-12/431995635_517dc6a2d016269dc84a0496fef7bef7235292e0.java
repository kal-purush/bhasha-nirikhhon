package com.polynomialprogram.app;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


public class PolynomialF2Test {
  private static Stream<Arguments> providerForPolynomialsForDivide() {
    List<Arguments> list = new ArrayList<Arguments>();
    list.add(
        Arguments.of(new PolynomialF2(4, 1, 0, 1, 5), new PolynomialLinkedList(1, 1), "x^2-x+2.0"));

    list.add(Arguments.of(new PolynomialF2(4, 10, 9, -8, 6, 0),
        new PolynomialLinkedList(1, 0, 1, 5), "10.0x+9.0"));

    list.add(Arguments.of(new PolynomialF2(4, 1, 0, 6, 0, 6), new PolynomialLinkedList(1, 0, 5),
        "x^2+1.0"));

    list.add(Arguments.of(new PolynomialF2(4, 4, -7, -11, 5), new PolynomialLinkedList(1, 4, 5),
        "4.0x-23.0"));

    list.add(
        Arguments.of(new PolynomialF2(3, 2, 5, -18), new PolynomialLinkedList(1, 1, 4), "2.0"));

    list.add(
        Arguments.of(new PolynomialF2(4, 1, 1, 0, 0), new PolynomialLinkedList(1, 1, -2), "x"));

    Arguments[] pols = new Arguments[list.size()];
    return Stream.of(list.toArray(pols));
  }


  @ParameterizedTest
  @MethodSource("providerForPolynomialsForDivide")
  public void testDivide(PolynomialF2 pol1, PolynomialLinkedList pol2, String expected) {
    PolynomialF1 result = pol1.dividePolynomialF2WithPolynomialLinkedList(pol2);
    assertEquals(expected, result.show());
  }

}