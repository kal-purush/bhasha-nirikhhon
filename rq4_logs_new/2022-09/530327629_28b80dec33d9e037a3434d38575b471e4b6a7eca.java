package Recursion;

import java.util.Arrays;

public class SumTriangle {
  //https://www.geeksforgeeks.org/sum-triangle-from-array/
     public static void main(String[] args) {
        int[] arr = {1, 2, 3, 4, 5};
     sumTriangle(arr);
   
     }

    private static void sumTriangle(int[] arr ) {
        //base condition
        if(arr.length<1){
            return;
        }
        int[]sumArray=new int[arr.length-1];
        // for (int i = 0; i < arr.length-1; i++) {
        
        //     int x=arr[i]+arr[i+1];
        //      sumArray[i]=x;
        // }
          helper(sumArray,arr,0);
        sumTriangle(sumArray);
            System.out.println(Arrays.toString(arr));
           
    }

    private static int[] helper(int[] sumArray, int[] arr, int index) {
      //base condition
      if(index==arr.length-1){
        return sumArray;
      }
        sumArray[index]=arr[index]+arr[index+1];
        return helper(sumArray,arr,index+1);
    }

}