package com.gowri.blitz.service.impl;/*
 * @author NaveenWodeyar
 * @date 02-03-2025
 */

import java.util.HashSet;

public class DuplicateElements {
    public static void main(String[] args) {
        int[] arr = {1, 2, 3, 4, 5, 2, 3, 6, 7};  // Example array
        findDuplicates(arr);
    }

    public static void findDuplicates(int[] arr) {
        HashSet<Integer> set = new HashSet<>();
        System.out.println("Duplicate elements are:");

        for (int i = 0; i < arr.length; i++) {
            if (!set.add(arr[i])) {  // If add returns false, it means element is already in the set
                System.out.println(arr[i]);
            }
        }
    }
}
