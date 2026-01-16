package challenges.sorting;

import static org.junit.Assert.assertArrayEquals;

public class IntSortTest {
    public void heapSortInt() {
        int[][] arrays = {
                {},
                {1},
                {2,1},
                {1,2},
                {3,2,1},
                {3,1,2},
                {2,1,3},
                {2,3,1},
                {1,3,2},
                {1,2,3}
        };

        int[][] expected = {
                {},
                {1},
                {1,2},
                {1,2},
                {1,2,3},
                {1,2,3},
                {1,2,3},
                {1,2,3},
                {1,2,3},
                {1,2,3},
        };

        for (int i = 0; i < arrays.length; i++) {
            HeapSort.heapSort(arrays[i]);
            assertArrayEquals(expected[i], arrays[i]);
        }

        int[][] arrays1 = {
                {10,7,5,3,2,1,9,8,4,6},
                {1,4,2,3,5,8,6,7,10,9},
        };

        int[] expected1 = {1,2,3,4,5,6,7,8,9,10};

        int[][] arrays2 = {
                {6,5,3,4,2,1},
                {3,1,2,6,5,4}
        };

        int[] expected2 = {1,2,3,4,5,6};

        for (int[] array : arrays1) {
            HeapSort.heapSort(array);
            assertArrayEquals(expected1, array);
        }

        for (int[] array : arrays2) {
            HeapSort.heapSort(array);
            assertArrayEquals(expected2, array) ;
        }
    }
}