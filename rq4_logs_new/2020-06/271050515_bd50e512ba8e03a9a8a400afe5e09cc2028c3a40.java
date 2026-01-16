package com.kodilla.stream.array;

import org.junit.Assert;
import org.junit.Test;
import java.util.stream.IntStream;

public class ArrayOperationsTestSuite {
    private int[] generateNumbers() {
        final int[] numbers = new int[6];
        return IntStream.range(0,numbers.length)
                .map(n ->numbers[n] = n)
                .toArray();
    }

    @Test
    public void testGetAverage() {
        //Given + When
        double average = ArrayOperations.getAverage(generateNumbers());

        //Then
        Assert.assertEquals(6, generateNumbers().length);
        Assert.assertEquals(2.5, average, 0.0001);
    }
}