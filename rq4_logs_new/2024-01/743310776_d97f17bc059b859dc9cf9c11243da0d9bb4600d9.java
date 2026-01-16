package com.vmelnyk.codesignal._20240117CountDigitAnagramPairs;

import com.vmelnyk.Util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Solution {

  long countDigitAnagrams(int[] a) {
    int n = a.length;
    int[] decoded = new int[n];

    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {

      final String s =
          Stream.of(Integer.toString(a[i]).split(""))
              .sorted(Comparator.reverseOrder())
              .collect(Collectors.joining());

      decoded[i] = Integer.parseInt(s);
    }
    System.out.println("Decoding takes " + (System.currentTimeMillis() - start));

    start = System.currentTimeMillis();

    Arrays.sort(decoded);
    long result = countUniquePairs(decoded, n);
    //    for (int i = 0; i < n; i++) {
    //      for (int j = i + 1; j < n; j++) {
    //        if (decoded[i] == decoded[j]) {
    //          result++;
    //        }
    //      }
    //    }

    System.out.println("Searching takes " + (System.currentTimeMillis() - start));

    System.out.println("Found: " + result);

    return result;
  }

  public static long countUniquePairs(int arr[], int n) {
    Map<Integer, Integer> hm = new HashMap<>();

    // Traversing through the array
    for (int i = 0; i < n; i++) {
      // Counting frequency
      hm.put(arr[i], hm.getOrDefault(arr[i], 0) + 1);
    }

    // To store the required count
    long count = 0;
    for (Map.Entry<Integer, Integer> m : hm.entrySet()) {
      int val = m.getValue();
      count += Util.permute(val, 2)/2;
    }

    return count;
  }
}