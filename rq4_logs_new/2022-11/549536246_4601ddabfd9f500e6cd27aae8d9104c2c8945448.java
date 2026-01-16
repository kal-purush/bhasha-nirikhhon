package com.fwhyn.myapplication.javatest;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;

public class CustomSetTest {

    @Test
    public void isSame() {
        Assert.assertTrue(new CustomSet().isSame(1, 1));
        Assert.assertTrue(new CustomSet().isSame("aaaa", "aaaa"));
        Assert.assertTrue(new CustomSet().isSame());
    }
}