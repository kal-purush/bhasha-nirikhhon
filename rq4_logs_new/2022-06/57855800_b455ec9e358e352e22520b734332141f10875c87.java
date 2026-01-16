package com.github.lucaseo90.ds.day01.array;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class No0001Test {

    private No0001 no0001 = new No0001();

    @Test
    public void example01() {
        Assert.assertEquals("[0, 1]", Arrays.toString(no0001.twoSum(new int[]{2, 7, 11, 15}, 9)));
    }

    @Test
    public void example02() {
        Assert.assertEquals("[1, 2]", Arrays.toString(no0001.twoSum(new int[]{3, 2, 4}, 6)));
    }

    @Test
    public void example03() {
        Assert.assertEquals("[0, 1]", Arrays.toString(no0001.twoSum(new int[]{3, 3}, 6)));
    }
}