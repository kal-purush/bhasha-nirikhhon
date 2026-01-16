package com.vmelnyk.geeks4geeks._20240124ShortestPrimePath;

import java.util.*;

class Solution {
  private final boolean[] primeList;

  Solution() {
    this.primeList = fetchPrimeList(10000);
  }

  int solve(int from, int to) {
    if (from == to) {
      return 0;
    }

    boolean[] forwardVisited = new boolean[10000];
    boolean[] backwardVisited = new boolean[10000];

    Queue<Integer> forward = new LinkedList<>();
    Queue<Integer> backward = new LinkedList<>();

    forward.add(from);
    forwardVisited[from] = true;

    backward.add(to);
    backwardVisited[to] = true;

    int level = 0;

    while (!forward.isEmpty() && !backward.isEmpty()) {
      int size = forward.size();
      for (int k = 0; k < size; k++) {
        String current = forward.poll().toString();

        for (int i = 0; i < 4; i++) {
          int start = i == 0 ? 1 : 0;
          for (int j = start; j <= 9; j++) {
            String next = current.substring(0, i) + j + current.substring(i + 1);
            int num = Integer.parseInt(next);

            if (primeList[num]) {
              if (backwardVisited[num]) {
                return level + 1;
              }

              if (!forwardVisited[num]) {
                forward.add(num);
                forwardVisited[num] = true;
              }
            }
          }
        }
      }

      level++;

      size = backward.size();
      for (int k = 0; k < size; k++) {
        String current = backward.poll().toString();

        for (int i = 0; i < 4; i++) {
          int start = i == 0 ? 1 : 0;
          for (int j = start; j <= 9; j++) {
            String next = current.substring(0, i) + j + current.substring(i + 1);
            int num = Integer.parseInt(next);

            if (primeList[num]) {
              if (forwardVisited[num]) {
                return level + 1;
              }

              if (!backwardVisited[num]) {
                backward.add(num);
                backwardVisited[num] = true;
              }
            }
          }
        }
      }

      level++;
    }

    return -1; // If no path is found
  }

  private boolean[] fetchPrimeList(int max) {
    boolean[] primes = new boolean[max];
    Arrays.fill(primes, true);
    for (int i = 2; i * i < max; i++) {
      if (primes[i]) {
        for (int j = i * i; j < max; j += i) {
          primes[j] = false;
        }
      }
    }
    return primes;
  }
}