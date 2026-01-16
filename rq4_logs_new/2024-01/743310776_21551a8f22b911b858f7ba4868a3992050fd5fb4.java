package com.vmelnyk.leetcode._1114PrintInOrder;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Foo {

  private final Lock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();

  private volatile int counter = 0;

  public Foo() {}

  public void first(Runnable printFirst) throws InterruptedException {
    lock.lock();
    try {
      while (counter%3!=0) {
        condition.await();
      }

      // printFirst.run() outputs "first". Do not change or remove this line.
      printFirst.run();

      counter++;

      condition.signalAll();

    } finally {
      lock.unlock();
    }
  }

  public void second(Runnable printSecond) throws InterruptedException {
    lock.lock();
    try {
      while (counter%3!=1) {
        condition.await();
      }

      // printSecond.run() outputs "second". Do not change or remove this line.
      printSecond.run();

      counter++;

      condition.signalAll();

    } finally {
      lock.unlock();
    }
  }

  public void third(Runnable printThird) throws InterruptedException {
    lock.lock();
    try {
      while (counter%3!=2) {
        condition.await();
      }

      // printThird.run() outputs "third". Do not change or remove this line.
      printThird.run();

      counter++;

      condition.signalAll();

    } finally {
      lock.unlock();
    }
  }
}