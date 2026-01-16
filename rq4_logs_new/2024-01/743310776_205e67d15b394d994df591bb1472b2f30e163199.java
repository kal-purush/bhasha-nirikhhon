package com.vmelnyk.leetcode._1115PrintFooBarAlternately;

class FooBar {
  private int n;

  private volatile int counter = 0;

  public FooBar(int n) {
    this.n = n;
  }

  public void foo(Runnable printFoo) throws InterruptedException {

    for (int i = 0; i < n; i++) {

      synchronized (this) {
        while (counter % 2 != 0) {
          wait();
        }

        // printFoo.run() outputs "foo". Do not change or remove this line.
        printFoo.run();

        counter++;

        notifyAll();
      }
    }
  }

  public void bar(Runnable printBar) throws InterruptedException {

    for (int i = 0; i < n; i++) {

      synchronized (this) {
        while (counter % 2 != 1) {
          wait();
        }

        // printBar.run() outputs "bar". Do not change or remove this line.
        printBar.run();

        counter++;

        notifyAll();
      }
    }
  }
}