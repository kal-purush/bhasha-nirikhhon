package plugin;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class CalculatorTest {

    public Calculator calculator;
    private String op;
    private List<Double> calculation;
    private Double expect;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { "division", new ArrayList<Double>(){{ add(15.0); add(3.0); }}, 5.0 },
            { "plus", new ArrayList<Double>(){{ add(1.0); add(2.0); }}, 3.0 },
            { "min", new ArrayList<Double>(){{ add(3.0); add(1.0); }}, 2.0 },
            { "multiply", new ArrayList<Double>(){{ add(3.0); add(2.0); }}, 6.0 },
            { "negation", new ArrayList<Double>(){{ add(3.0); }}, -3.0 },
            { "plus", new ArrayList<Double>(){{ add(3.0); add(3.0); }}, 6.0 }
        });
    }

    public CalculatorTest(String op, List<Double> calculation, Double expect) {
        this.op = op;
        this.calculation = calculation;
        this.expect = expect;
    }

    @Before
    public void setUp() throws Exception {
        calculator = new Calculator();
    }

    @Test
    public void testCalc() throws Exception {
        assertEquals(expect, (Double) new Calculator().calc(this.op, this.calculation));
    }
}