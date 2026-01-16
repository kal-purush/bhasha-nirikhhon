package com.vmelnyk.leetcode._1117BuildingH2O;

import java.util.concurrent.Semaphore;

class H2O {
  private int hydrogenCount = 0;
  private final Object lock = new Object();

  public H2O() {
  }

  public void hydrogen(Runnable releaseHydrogen) throws InterruptedException {
    synchronized(lock) {
      while (hydrogenCount == 2) { // Wait if two hydrogens are already released
        lock.wait();
      }
      releaseHydrogen.run();
      hydrogenCount++;
      if (hydrogenCount == 2) { // If two hydrogens are released, reset for oxygen
        lock.notifyAll();
      }
    }
  }

  public void oxygen(Runnable releaseOxygen) throws InterruptedException {
    synchronized(lock) {
      while (hydrogenCount != 2) { // Wait for two hydrogens to be released
        lock.wait();
      }
      releaseOxygen.run();
      hydrogenCount = 0; // Reset hydrogen count after oxygen is released
      lock.notifyAll(); // Notify all waiting threads
    }
  }
}

class H2O1 {

  private final Semaphore hydrogen = new Semaphore(2);
  private final Semaphore oxygen = new Semaphore(0);

  public H2O1() {}

  public void hydrogen(Runnable releaseHydrogen) throws InterruptedException {
    hydrogen.acquire();

    // releaseHydrogen.run() outputs "H". Do not change or remove this line.
    releaseHydrogen.run();

    oxygen.release();
  }

  public void oxygen(Runnable releaseOxygen) throws InterruptedException {
    oxygen.acquire(2);

    // releaseOxygen.run() outputs "O". Do not change or remove this line.
    releaseOxygen.run();

    hydrogen.release(2);
  }
}