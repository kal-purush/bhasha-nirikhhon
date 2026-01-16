package com.ab.practice.sorting;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BubbleSortTest {
    SortInterface sort = new BubbleSort();
    private final int[] input;
    private final int[] expected;

    public BubbleSortTest(int[] input, int[] expected) {
        this.input = input;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection primeNumbers() {
        return Arrays.asList(new Object[][] {
                { new int[] {2, 5, 1, 4, 8, 9, 3, 6, 7, 10}, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10} },
                { new int[0], new int[0] },
                { new int[] {1}, new int[] {1} },
                { new int[] {2, 5, 1, 4, 8, 9, 3, 6, 7, 10, 4, 5, 7}, new int[] {1, 2, 3, 4, 4, 5, 5, 6, 7, 7, 8, 9, 10} },
                { null, null },
                { new int[] {1, 2, 3, 4, 5}, new int[] {1, 2, 3, 4, 5}}
        });
    }

    @Test
    public void testSorting() {
        Assert.assertArrayEquals(expected, sort.sort(input));
    }
}