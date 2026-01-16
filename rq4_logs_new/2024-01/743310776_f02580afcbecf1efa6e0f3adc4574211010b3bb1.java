package com.vmelnyk.leetcode._1195FizzBuzzMultithreaded;

import java.util.function.IntConsumer;

class FizzBuzz {
  private final Object lock = new Object();
  private final int n;

  private int count = 1;

  public FizzBuzz(int n) {
    this.n = n + 1;
  }

  // printFizz.run() outputs "fizz".
  public void fizz(Runnable printFizz) throws InterruptedException {
    synchronized (lock) {
      while (count < n) {
        if (count % 3 == 0 && count % 5 != 0) {
          printFizz.run();
          count++;
          lock.notifyAll();
        } else {
          lock.wait();
        }
      }
    }
  }

  // printBuzz.run() outputs "buzz".
  public void buzz(Runnable printBuzz) throws InterruptedException {
    synchronized (lock) {
      while (count < n) {
        if (count % 3 != 0 && count % 5 == 0) {
          printBuzz.run();
          count++;
          lock.notifyAll();
        } else {
          lock.wait();
        }
      }
    }
  }

  // printFizzBuzz.run() outputs "fizzbuzz".
  public void fizzbuzz(Runnable printFizzBuzz) throws InterruptedException {
    synchronized (lock) {
      while (count < n) {
        if (count % 15 == 0) {
          printFizzBuzz.run();
          count++;
          lock.notifyAll();
        } else {
          lock.wait();
        }
      }
    }
  }

  // printNumber.accept(x) outputs "x", where x is an integer.
  public void number(IntConsumer printNumber) throws InterruptedException {
    synchronized (lock) {
      while (count < n) {
        if (count % 3 != 0 && count % 5 != 0) {
          printNumber.accept(count);
          count++;
          lock.notifyAll();
        } else {
          lock.wait();
        }
      }
    }
  }
}