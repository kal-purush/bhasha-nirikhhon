// Java code for Kth largest element
// in an array
import java.util.Arrays;
import java.util.Collections;

public class ArrayFindKthLargestElementInAnArrayUsingSort {
        // Function to return K'th smallest
        // element in a given array
        public static int kthSmallest(Integer[] arr, int K)
        {
            // Sort the given array, reverse the array.
            //Arrays.sort(arr);
            Arrays.sort(arr);
            Collections.reverse(Arrays.asList(arr));
            System.out.println(Arrays.asList(arr));

            // Return K'th element in
            // the sorted array
            return arr[K - 1];
        }

        public static void main(String[] args)
        {
            Integer arr[] = new Integer[] { 12, 3, 5, 7, 19 };
            int K = 1;

            // Function call
            System.out.print("K'th largest element is "
                    + kthSmallest(arr, K));
        }
}

