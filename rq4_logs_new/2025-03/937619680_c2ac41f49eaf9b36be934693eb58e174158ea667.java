package com.gowri.blitz.service.impl;/*
 * @author NaveenWodeyar
 * @date 03-03-2025
 */

public class EvenNumbers {
    public static void main(String[] args) {
        int start = 1;  // Start of the range
        int end = 20;   // End of the range

        System.out.println("Even numbers between " + start + " and " + end + ":");

        // Loop through the range of numbers
        for (int i = start; i <= end; i++) {
            // Check if the number is even
            if (i % 2 == 0) {
                System.out.println(i);
            }
        }
    }
}
