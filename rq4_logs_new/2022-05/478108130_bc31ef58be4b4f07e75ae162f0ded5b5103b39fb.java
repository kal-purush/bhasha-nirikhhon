package net.absoft.discriminant;

import net.absoft.solution.QuadraticEquation;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class QuadraticEquationTest {

    public Map<String, Object> expectedErorTemplate(int code, String message){
        Map<String, Object> e = new HashMap<>();
        e.put("errorCode", code);
        e.put("errorMessage", message);
        return e;
    }

    public Map<String, Object> expectedSolutionTemplate(double x1, double x2){
        Map<String, Object> s = new HashMap<>();
        s.put("x1", x1);
        s.put("x2", x2);
        return s;
    }

    @Test(description = "Test#1: D > 0")
    public void testEquationWith2DifferentSQRT() {
        Map<String, Object> actual = new QuadraticEquation(1,-4,2).solve();
        Assert.assertEquals(actual, expectedSolutionTemplate(2 + Math.sqrt(2.0), 2 - Math.sqrt(2.0)));
    }

    @Test(description = "Test#2: D == 0")
    public void testEquationWith2SameSQRT() {
        Map<String, Object> actual = new QuadraticEquation(1, 12, 36).solve();
        Assert.assertEquals(actual, expectedSolutionTemplate(-6.0, -6.0));
    }
    //Custom solution implemented after clarification with reviewer
    @Test(description = "Test# 3.0 : D < 0: ERR_103: Discriminant less than ZERO: D < 0")
    public void testEquationWithoutSQRT() {
        Map<String, Object> actual = new QuadraticEquation(3, -4, 2).solve();
        Assert.assertEquals(actual, expectedErorTemplate(103, "ERR_103: Discriminant less than ZERO: D < 0"));
    }

    @Test(description = "Test# 1.1 : D > 0 (double check)")
    public void testEquationWith2DifferentSQRT_1() {
        Map<String, Object> actual = new QuadraticEquation(1, 9, 18).solve();
        Assert.assertEquals(actual, expectedSolutionTemplate(-3.0, -6.0));
    }

    @Test(description = "Test# 1.2 : D > 0 (with decimal sqrt)")
    public void testEquationWith2DifferentSQRT_2() {
        Map<String, Object> actual = new QuadraticEquation(2, 3, -5).solve();
        Assert.assertEquals(actual, expectedSolutionTemplate(1.0, -2.5));
   }

    @Test(description = "Test# 1.3 : D > 0 (with decimal sqrt: 5/3)")
    public void testEquationWith2DifferentSQRT_3() {

        Map<String, Object> actual = new QuadraticEquation(-3, 5, 0).solve();
        Assert.assertEquals(actual, expectedSolutionTemplate(-0.0, (double) 5 / 3));
    }

    @Test(description = "Test# 2.1 : b == 0, c == 0: regression after implementation custom logic")
    public void testEquationWith2SameSQRT_1() {
        Map<String, Object> actual = new QuadraticEquation(1, 0, 0).solve();
        Assert.assertEquals(actual, expectedSolutionTemplate(0.0, 0.0));
    }

    @Test(description = "Test# 4.0 : a == 0, linear equation: ERR_102: Provided linear equation instead of quadratic")
    public void testErrorForLinearEquation() {
        Map<String, Object> actual = new QuadraticEquation(0, 3, -5).solve();
        Assert.assertEquals(actual, expectedErorTemplate(102, "ERR_102: Provided linear equation instead of quadratic"));
    }

    @Test(description = "Test# 5.0 : a == 0, b == 0 incomplete: ERR_101: Incomplete quadratic equation")
    public void testErrorForIncompleteQuadraticEquation() {
        Map<String, Object> actual = new QuadraticEquation(0, 0, -5).solve();
        Assert.assertEquals(actual, expectedErorTemplate(101, "ERR_101: Incomplete quadratic equation"));
    }


    @Test(description = "Test# 5.1 : a == 0, c == 0 incomplete: ERR_101: Incomplete quadratic equation")
    public void testErrorForIncompleteQuadraticEquation_1() {
        Map<String, Object> actual = new QuadraticEquation(0, -3, 0).solve();
        Assert.assertEquals(actual, expectedErorTemplate(101, "ERR_101: Incomplete quadratic equation"));
    }

    @Test(description = "Test# 5.2 : a == 0, b == 0, c == 0 incomplete: ERR_101: Incomplete quadratic equation")
    public void testErrorForIncompleteQuadraticEquation_2() {
        Map<String, Object> actual = new QuadraticEquation(0, 0, 0).solve();
        Assert.assertEquals(actual, expectedErorTemplate(101, "ERR_101: Incomplete quadratic equation"));
    }
}