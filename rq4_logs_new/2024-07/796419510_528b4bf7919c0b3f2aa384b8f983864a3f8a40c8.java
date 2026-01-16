package org.jal.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.IntBinaryOperator;

import org.junit.jupiter.api.Test;

public class RandTwoWayStrategyTestExample {
  @Test
  public void testSuccess() {
    Integer[] arr = new Integer[] { 4, 5, 6, 2, 3 };
    Comparator<Integer> identity = Comparator.comparing(v -> v);

    IntBinaryOperator chooseBegin = (a, b) -> a;
    PartitionStrategy<Integer> strat = new RandTwoWayStrategy<>(chooseBegin);

    int pivot = strat.partition(arr, 0, arr.length, identity);
    int pivotVal = arr[pivot];

    assertEquals(4, pivotVal);
    assertTrue(Arrays.stream(arr, 0, pivot).allMatch(v -> v <= pivotVal));
    assertTrue(Arrays.stream(arr, pivot+1, arr.length).allMatch(v -> v >= pivotVal));
  }
}