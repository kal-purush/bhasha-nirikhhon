package LeetCode_Daily;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class Unique_Number_of_Occurrences {
    public static void main(String[] args) {
        int[] exampleArray = {1, 2, 2, 1, 1, 3};
        System.out.println(uniqueOccurrences(exampleArray));

    }

    public static boolean uniqueOccurrences(int[] arr) {
        HashMap<Integer, Integer> hashMap = new HashMap<>();

        for (int i = 0; i < arr.length; i++) {
            if (!hashMap.containsKey(arr[i])) {
                hashMap.put(arr[i], 0);
            } else {
                int currentValue = hashMap.get(arr[i]);
                currentValue++;
                hashMap.put(arr[i], currentValue);
            }
        }

        HashSet<Integer> hashSet = new HashSet<>();

        for (Map.Entry<Integer, Integer> entry : hashMap.entrySet()) {
            hashSet.add(entry.getValue());
        }

        if (hashSet.size() == hashMap.size()) {
            return true;
        } else {
            return false;
        }
    }

}