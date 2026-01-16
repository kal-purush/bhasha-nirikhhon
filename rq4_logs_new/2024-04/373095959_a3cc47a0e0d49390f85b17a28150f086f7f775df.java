package org.frank.cucumber.test;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import static org.junit.Assert.assertEquals;

public class CalculatorSteps {
    private Calculator calculator;
    private int result;

    @Given("the calculator is run")
    public void the_calculator_is_run() {
        calculator = new Calculator();
    }

    @When("I add {int} and {int}")
    public void i_add_and(int num1, int num2) {
        result = calculator.add(num1, num2);
    }

    @Then("the result should be {int}")
    public void the_result_should_be(int expected) {
        assertEquals(expected, result);
    }
}

class Calculator {
    public int add(int a, int b) {
        return a + b;
    }
}