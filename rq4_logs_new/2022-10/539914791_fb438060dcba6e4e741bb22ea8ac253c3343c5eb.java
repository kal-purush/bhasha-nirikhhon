package org.example.algo;
import java.util.*;

public class TwoNumberSum {

    // time complexity Big O (N)
    public static int[] twoNumberSumV2(int[] array, int targetSum) {
        ArrayList<Integer> result = new ArrayList<>();
        int i = 0;
        int diff = 0;
        HashMap<Integer, Integer> map = new HashMap<>();
        for(i = 0; i < array.length; i++) {
            map.put(array[i], i);
        }
        for(i = 0; i < array.length; i++) {
            diff = targetSum - array[i];
            if(map.get(diff) != null && i != map.get(diff) && array[i] + array[map.get(diff)] == targetSum) {
                result.add(array[i]);
                result.add(array[map.get(diff)]);
                break;
            }
        }
        if(result.size() == 2 ) {
            return result.stream()
                    .mapToInt(Integer::intValue)
                    .toArray();
        }
        return new int[0];
    }

    // time complexity Big O (N)
    public static int[] twoNumberSumV2Index(int[] array, int targetSum) {
        ArrayList<Integer> result = new ArrayList<>();
        int i = 0;
        int diff = 0;
        HashMap<Integer, Integer> map = new HashMap<>();
        for(i = 0; i < array.length; i++) {
            map.put(array[i], i);
        }
        for(i = 0; i < array.length; i++) {
            diff = targetSum - array[i];
            if(map.get(diff) != null && i != map.get(diff) && array[i] + array[map.get(diff)] == targetSum) {
                result.add(i);
                result.add(map.get(diff));
                break;
            }
        }
        if(result.size() == 2 ) {
            return result.stream()
                    .mapToInt(Integer::intValue)
                    .toArray();
        }
        return new int[0];
    }

    // time complexity Big O (N^2)
    public static int[] twoNumberSumIndex(int[] array, int targetSum) {
        int[] results = new int[2];

        for(int i = 0; i< array.length-1; i++) {
            for(int j = 1; j < array.length; j++) {
                if(i != j && array[i] + array[j] == targetSum) {
                    results[0] = i;
                    results[1] = j;
                    return results;
                }
            }
        }
        return new int[0];
    }

    // time complexity Big O (N^2)
    public static int[] twoNumberSum(int[] array, int targetSum) {
        int[] results = new int[2];

        for(int i = 0; i< array.length-1; i++) {
            for(int j = 1; j < array.length; j++) {
                if(i != j && array[i] + array[j] == targetSum) {
                    results[0] = array[i];
                    results[1] = array[j];
                    return results;
                }
            }
        }
        return new int[0];
    }

    public static void main(String[] args) {
        int[] array = {3, 5, -1, 8, 11, 1, -1 , 6};
        int targetSum = 10;

        int[] array2 = {4, 6};
        System.out.println(Arrays.toString(TwoNumberSum.twoNumberSumV2Index(array, targetSum)));
        System.out.println(Arrays.toString(TwoNumberSum.twoNumberSumV2Index(array2, targetSum)));
    }
}