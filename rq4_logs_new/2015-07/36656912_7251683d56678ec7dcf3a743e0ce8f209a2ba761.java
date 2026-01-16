package j8spec.examples;

import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;

import static j8spec.J8Spec.describe;
import static j8spec.J8Spec.it;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(J8SpecRunner.class)
public class CalculatorTest {{

    describe("operations", () -> {
        it("adds two integers", () -> {
            Calculator calculator = new Calculator();

            assertThat(calculator.add(1, 3), is(4));
        });
    });

}}