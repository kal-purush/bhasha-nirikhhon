package org.jal.sorting.strategy.arrays;

import static org.jal.util.Arrays.swap;

import java.util.Comparator;

import org.jal.collections.heap.Heapifier;

public class HeapStrategy<T> implements ArraySortStrategy<T> {
  public T[] sortArray(T[] arr, int begin, int end, Comparator<T> comp) {
    Heapifier<T> heapifier = new Heapifier<>(i -> arr[i], (i, value) -> arr[i] = value, comp);

    int root = begin;
    int last = end-1;
    heapifier.heapify(root, last);

    for (int i = last; i > begin; --i) {
      // move the greatest value to the last in the interval [begin, i+1)
      swap(arr, begin, i);

      // heapify for the reduced interval [begin, i)
      heapifier.siftDown(begin, begin, i-1);
    }

    return arr;
  }
}