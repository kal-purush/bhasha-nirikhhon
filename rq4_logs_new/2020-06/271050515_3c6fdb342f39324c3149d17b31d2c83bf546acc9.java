package com.kodilla.testing.collection.fibonacci;

import com.kodilla.testing.fibonacci.Fibonacci;
import org.junit.Assert;
import org.junit.Test;

public class FibonacciTestSuite {

    @Test
    public void testZero() {
        Fibonacci fibonacci = new Fibonacci();
        Assert.assertEquals(0, fibonacci.f(0));
    }

    @Test
    public void testOne() {
        Fibonacci fibonacci = new Fibonacci();
        Assert.assertEquals(1, fibonacci.f(1));
    }

    @Test
    public void testTwelve() {
        Fibonacci fibonacci = new Fibonacci();
        Assert.assertEquals(144, fibonacci.f(12));
    }

}