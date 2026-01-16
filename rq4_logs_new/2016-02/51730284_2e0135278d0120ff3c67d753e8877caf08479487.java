import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by sulabhc on 2/12/16.
 */
public class sumof3numbers {

    public static void main(String[] args){
        int[] array = {1,2,2,2,3,4,5,6,7,8};
        //int[] array = {1,0,-1,-2,-3,2,3,3};
        find3Numbers(triplicateElement(array), 23);
       // tripletSumWithSorting(array, 4);
    }

    static int[] triplicateElement(int[] arr){
        int[] newArray =new int[arr.length*3];
        for (int i=0; i< arr.length; i++){
            newArray[3*i] = arr[i];
            newArray[3*i+1] = arr[i];
            newArray[3*i+2] = arr[i];
        }
        return newArray;
    }
    static void tripletSumZero(int[] array, int sum) {
        int len = array.length;
        int count =0;
        for (int i = 0; i < len - 2; i++) {
            for (int j = i + 1; j < len - 1; j++) {
                for (int k = j + 1; k < len; k++) {
                    count++;
                    if (array[i] + array[j] + array[k] == sum)
                        System.out.format("Triplet is %d, %d, %d \n", array[i], array[j], array[k]);
                }
            }
        }
        System.out.println(count);
    }

    static void tripletSumWithSorting(int[] data, int sum){

        Arrays.sort(data);
        for (int i = 0; i < data.length; i++) {
            for (int j = i + 1; j < data.length; j++) {
                int[] newArray = Arrays.copyOfRange(data, j + 1, data.length);
                int pos = binarySearch(newArray, sum - ((data[i] + data[j])));
                //int pos = Arrays.binarySearch(data, j + 1, data.length, sum - (data[i] + data[j]));
                if (pos >= 0) {
                    System.out.format("%d %d %d", pos, newArray[pos], sum - (data[i] + data[j]));
                    System.out.printf("Zero-sum values are [%d,%d,%d]\n", data[i], data[j], newArray[pos]);
                }
            }
        }

    }

    static int binarySearch(int[] arr, int key){

        int left = 0;
        int right = arr.length - 1;
        while (left <= right){
            int mid = (left + right)/2;
            if (key==arr[mid]) return mid;
            else if (key < arr[mid]) right = mid - 1;
            else if (key > arr[mid]) left = mid + 1;
        }
        return -1;
    }

    static void find3Numbers(int A[], int sum)
    {
        int l, r;

        Arrays.sort(A);

        int arr_size = A.length;

    /* Now fix the first element one by one and find the
       other two elements */
        for (int i = 0; i < arr_size - 2; i++)
        {

            // To find the other two elements, start two index variables
            // from two corners of the array and move them toward each
            // other
            l = i + 1; // index of the first element in the remaining elements
            r = arr_size-1; // index of the last element
            while (l < r)
            {
                if( A[i] + A[l] + A[r] == sum)
                {
                    System.out.format("Triplet is %d, %d, %d \n", A[i], A[l], A[r]);
                    l++;r--;
                }
                else if (A[i] + A[l] + A[r] < sum)
                    l++;
                else // A[i] + A[l] + A[r] > sum
                    r--;
            }
        }
       // return false;
    }



}