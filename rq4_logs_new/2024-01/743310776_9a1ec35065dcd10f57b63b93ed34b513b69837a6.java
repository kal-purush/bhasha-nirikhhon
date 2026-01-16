package com.vmelnyk.leetcode._1116PrintZeroEvenOdd;

import java.util.function.IntConsumer;

class ZeroEvenOdd {
  private final int n;
  private int state = 0;

  public ZeroEvenOdd(int n) {
    this.n = n;
  }

  // printNumber.accept(x) outputs "x", where x is an integer.
  public synchronized void zero(IntConsumer printNumber) throws InterruptedException {
    for (int i = 0; i < n; i++) {
      while (state != 0) {
        wait();
      }

      printNumber.accept(0);

      state = i % 2 == 0 ? 1 : 2;

      notifyAll();
    }
  }

  public synchronized void even(IntConsumer printNumber) throws InterruptedException {
    for (int i = 2; i <= n; i += 2) {
      while (state != 2) {
        wait();
      }

      printNumber.accept(i);

      state = 0;

      notifyAll();
    }
  }

  public synchronized void odd(IntConsumer printNumber) throws InterruptedException {
    for (int i = 1; i <= n; i += 2) {
      while (state != 1) {
        wait();
      }

      printNumber.accept(i);

      state = 0;

      notifyAll();
    }
  }
}