import java.io.*;
import java.util.*;
import java.text.*;
import java.math.*;
import java.util.regex.*;

public class Solution {

    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        int n = in.nextInt();
        int a[] = new int[n];
        for(int a_i=0; a_i < n; a_i++){
            a[a_i] = in.nextInt();
        }
        bubbleSort(a);
    }
    
    static void bubbleSort(int[] arr){
        int numSwap = 0;
        boolean flag = false;
        while(!flag){
            int e = 0;
            int lastNum = arr.length-1;
            flag = true;
            while(e<lastNum){
                if(arr[e] > arr[e+1]) {
                    swap(arr,e);
                    numSwap++;
                    flag = false;
                }
                e++;
            }
            lastNum--;
        }
        System.out.println("Array is sorted in " + numSwap + " swaps.");
        System.out.println("First Element: " + arr[0]);
        System.out.println("Last Element: " + arr[arr.length-1]);
    }
    
    static void swap(int[] arr, int e){
        int temp = arr[e];
        arr[e] = arr[e+1];
        arr[e+1] = temp;
    }
}