package com.github.lucaseo90.easy;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class No0977Test {

    No0977 no0977 = new No0977();

    @Test
    public void testExample01() {
        Assert.assertEquals("[0, 1, 16, 100]", Arrays.toString(no0977.sortedSquares(new int[]{-4, -1, 0, 10})));
    }

    @Test
    public void testExample02() {
        Assert.assertEquals("[4, 9, 9, 49, 121]", Arrays.toString(no0977.sortedSquares(new int[]{-7, -3, 2, 3, 11})));
    }

}