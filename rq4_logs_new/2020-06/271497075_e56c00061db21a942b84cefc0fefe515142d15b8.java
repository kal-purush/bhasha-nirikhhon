package com.example.springboot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CalculatorControllerTests {

    @Test
    public void testApplyOpPlus() {
        //write at least 5 methods to cover those 4 branches + exception; cover all logic
        Assertions.assertEquals(12, CalculatorController.applyOp("+", 10, 2));
    }

    @Test
    public void testApplyOpMinus() {
        Assertions.assertEquals(8, CalculatorController.applyOp("-", 10, 2));
    }

    @Test
    public void testApplyOpMultiply() {
        Assertions.assertEquals(20, CalculatorController.applyOp("*", 10, 2));
    }

    @Test
    public void testApplyOpDivide() {
        Assertions.assertEquals(10, CalculatorController.applyOp("/", 20, 2));
    }

    @Test
    public void testApplyOpDivideAndRound() {
        Assertions.assertEquals(7, CalculatorController.applyOp("/", 15, 2));
    }

    @Test
    public void testApplyOpDivideByZero() {
        Exception exception = Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            CalculatorController.applyOp("/", 10, 0);
        });
        String expectedMessage = "Cannot divide by 0";
        String actualMessage = exception.getMessage();
        Assertions.assertTrue(actualMessage.contains(expectedMessage));
    }

}