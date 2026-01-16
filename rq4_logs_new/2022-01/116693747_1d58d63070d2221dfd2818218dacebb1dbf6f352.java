package com.tsunazumi.algorithms.hackerrank;

public class SavePrisoner {

  public static void main(String[] args) {

    int result = saveThePrisoner(4,6,2);
    // 1,2,3,
    System.out.println(result);
  }

  // n prisoners
  // m candies
  // s starting prisioner
  public static int saveThePrisoner(int n, int m, int s) {
    int result = s + m -1;
    result %= n;
    if (result == 0) {
      return n;
    }
    return result;
  }
}