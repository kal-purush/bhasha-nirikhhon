package com.study.test.algorithm;

import com.study.test.algorithm.impl.BubbleSort;
import com.study.test.algorithm.impl.InsertSort;
import com.study.test.algorithm.impl.QuickSort;
import com.study.test.algorithm.impl.SelectionSort;
import org.junit.Test;

public class SortTest {

    @Test
    public void quickSort() {
        Sort sort = new QuickSort();
        int[] arr = new int[]{5, 1, 7, 3, 9, 2, 6};
        arr = sort.sort(arr);
        for (int i : arr) {
            System.out.println(i);
        }
    }

    @Test
    public void selectionSort() {
        Sort sort = new SelectionSort();
        int[] arr = new int[]{5, 1, 7, 3, 9, 2, 6};
        arr = sort.sort(arr);
        for (int i : arr) {
            System.out.println(i);
        }
    }

    @Test
    public void bubbleSort() {
        Sort sort = new BubbleSort();
        int[] arr = new int[]{5, 1, 7, 3, 9, 2, 6};
        arr = sort.sort(arr);
        for (int i : arr) {
            System.out.println(i);
        }
    }

    @Test
    public void insertSort() {
        Sort sort = new InsertSort();
        int[] arr = new int[]{5, 1, 7, 3, 9, 2, 6};
        arr = sort.sort(arr);
        for (int i : arr) {
            System.out.println(i);
        }
    }
}