package com.vmelnyk.geeks4geeks._20240125FractionalKnapsack;

import java.util.Arrays;
import java.util.Comparator;

class Solution {
  // Function to get the maximum total value in the knapsack.
  double fractionalKnapsack(int weigth, Item[] arr, int n) {
    // Your code here
    Arrays.sort(arr, Comparator.comparing((Item a) -> (double) a.value / a.weight).reversed());

    double result = 0;
    for (Item item : arr) {
      if (item.weight <= weigth) {
        result += item.value;
        weigth -= item.weight;
      } else {
        result += (double) (weigth * item.value) / item.weight;
        break;
      }
    }
    return result;
  }
}

final class Item {
  int value, weight;

  Item(int x, int y) {
    this.value = x;
    this.weight = y;
  }
}