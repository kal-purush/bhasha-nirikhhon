/**
 * Created by shobhitgarg on 2/4/19.
 * given an array of size N-1 and numbers containing numbers from 1 to N, find the missing number
 */
public class MissingNumberInArray {

    public static void main(String[] args) {
        MissingNumberInArray missingNumberInArray = new MissingNumberInArray();
        int arr[] = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 10};
        System.out.println(missingNumberInArray.findMissingNumber(arr));
    }

    int findMissingNumber(int arr[])    {
        int size = arr.length;
        int maxNumber = size + 1;
        int remainingSum = maxNumber* (maxNumber+1)/2;
        for(int i : arr)    {
            remainingSum -=i;
        }
        return remainingSum;
    }

}