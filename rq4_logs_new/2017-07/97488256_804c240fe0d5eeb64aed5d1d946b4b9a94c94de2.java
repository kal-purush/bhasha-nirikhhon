package com.allstate.compozed.springplayground.math;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by charlesellison on 7/17/17.
 */
public class MathModelTest {
    @Test
    public void getSquare() throws Exception {
    }

    @Test
    public void getFactorial() throws Exception {
    }

    @Test
    public void getFibonacciReturns13WhenGiven7() throws Exception {
        MathModel math = new MathModel(7);

        assertEquals(13, math.getFibonacci());
    }

    @Test
    public void getFibonacciReturns0WhenGiven0() throws Exception {
        MathModel math = new MathModel(0);

        assertEquals(0, math.getFibonacci());
    }

    @Test
    public void getFibonacciReturns75025WhenGiven25() throws Exception {
        MathModel math = new MathModel(25);

        assertEquals(75025, math.getFibonacci());
    }

}