package com.vmelnyk;

import java.util.Random;

/** Usage: int[] array = {1, 2, 3}; Util.shuffle(array); */
public class Util {

  private static Random random;

  /** Code from method java.util.Collections.shuffle(); */
  public static void shuffle(int[] array) {
    if (random == null) {
      random = new Random();
    }
    int count = array.length;
    for (int i = count; i > 1; i--) {
      swap(array, i - 1, random.nextInt(i));
    }
  }

  private static void swap(int[] array, int i, int j) {
    int temp = array[i];
    array[i] = array[j];
    array[j] = temp;
  }

  public static <T> void shuffle(T[] array) {
    if (random == null) {
      random = new Random();
    }
    int count = array.length;
    for (int i = count; i > 1; i--) {
      swap(array, i - 1, random.nextInt(i));
    }
  }

  private static <T> void swap(T[] array, int i, int j) {
    T temp = array[i];
    array[i] = array[j];
    array[j] = temp;
  }

  public static long sumOfPowers(long x) {
    long c = 0;
    while (x % 2 == 0) {
      c++;
      x /= 2;
    }
    for (long i = 3; i <= Math.sqrt(x); i += 2) {
      while (x % i == 0) {
        c++;
        x /= i;
      }
    }
    if (x > 2) {
      c++;
    }
    return c;
  }
}