package src.main.java.array;
//Google Mock Interview
//We are given an array A consisting of N integers. In one move, we can choose any element in this array and replace it with any value.
//We are interested in the longest consistent segment of identical elements. For example, given an array A = [3, -3, 3, 3, 3, 1, -3], the segment that we are looking for is [3, 3, 3].
//
//What is the maximum length of such a segment that we can achieve by performing at most three moves on the array?
//
//Write a function:
//class Solution { public int solution(int[] A); }
//
//that, given an array A of N integers, returns the maximum length of a consistent segment of identical elements in the array A that can be obtained after replacing up to three elements in the array.
//
//        Examples:
//        Given A = [-9, 8], your function should return 2. One of the correct ways to obtain such a solution is to use one move and change 8 into -9.
//        Given A = [1, 1, -10, 3, -10, 3, -10], your function should return 6. By performing three moves we can obtain the following array: [1, -10, -10, -10, -10, -10, -10].
//        Given A = [2, 3, 3, 3, 3, 1], your function should return 6. We can perform two moves to replace the first and last elements with 3s.
//        Given A = [3, 3, 2, 1, 2, 2, 9, 3, 3], your function should return 6. By performing three moves we can obtain the following array: [3, 3, 2, 2, 2, 2, 2, 2, 3].
//
//        Assume that:
//        N is an integer within the range [1..100];
//        each element of array A is an integer within the range [−10..10].

import java.util.HashMap;
import java.util.Map;

public class MaxLengthSlideWindow {
    public static void main(String[] args) {
        //input
        int[] A = {1, 1, -10, 3, -10};
        System.out.println(isValid(A));
    }

    //check different lengths for valid replacements and return the Max length
    public int solution(int[] A){
        int maxLength = 0;

        return maxLength;
    }

    //true, if we can replace at most 3 elements and get all identical elements
    public static boolean isValid(int[] A){
        HashMap<Integer, Integer> hash = new HashMap();

        //creating the hashmap
        for(int i=0; i<A.length ; i++){
            if(!hash.containsKey(A[i])){
                hash.put(A[i],1);
            }else{
                int newVal = hash.get(A[i]) + 1;
                hash.put(A[i], newVal);
            }
        }

        //loop through hash map to ascertain if we can get a valid soln
        // how: arrayLength - value <= 3
        //method 1
        for(Map.Entry<Integer, Integer> item : hash.entrySet()){
            int arrayLength = A.length;
            if(arrayLength - item.getValue()<=3){
                return true;
            }
        }

        return false;
    }
}