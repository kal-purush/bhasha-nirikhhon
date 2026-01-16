package com.vmelnyk.leetcode._1226TheDiningPhilosophers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

class DiningPhilosophers2 {
  private final List<Semaphore> forks;
  private final int philosophers;

  public DiningPhilosophers2() {
    this(5);
  }

  public DiningPhilosophers2(int philosophers) {
    this.philosophers = philosophers;
    this.forks = new ArrayList<>();
    for (int p = 0; p < philosophers; p++) {
      this.forks.add(new Semaphore(1));
    }
  }

  // call the run() method of any runnable to execute its code
  public void wantsToEat(
      int philosopher,
      Runnable pickLeftFork,
      Runnable pickRightFork,
      Runnable eat,
      Runnable putLeftFork,
      Runnable putRightFork)
      throws InterruptedException {

    int left = (philosophers + philosopher - 1) % philosophers;
    int right = (philosopher + 1) % philosophers;

    System.out.println("Philosopher " + philosopher + " wants to eat!");
    forks.get(left).acquire();
    forks.get(right).acquire();

    pickLeftFork.run();
    pickRightFork.run();
    eat.run();
    putLeftFork.run();
    putRightFork.run();

    System.out.println("Philosopher " + philosopher + " completes eating.");
    forks.get(left).release();
    forks.get(right).release();
  }
}

class DiningPhilosophers {
  private final ReentrantLock[] forks;
  private final int philosophers;

  public DiningPhilosophers() {
    this(5);
  }

  public DiningPhilosophers(int philosophers) {
    this.philosophers = philosophers;
    this.forks = new ReentrantLock[philosophers];
    for (int i = 0; i < philosophers; i++) {
      this.forks[i] = new ReentrantLock();
    }
  }

  public void wantsToEat(
      int philosopher,
      Runnable pickLeftFork,
      Runnable pickRightFork,
      Runnable eat,
      Runnable putLeftFork,
      Runnable putRightFork)
      throws InterruptedException {
    final int left = (philosophers + philosopher - 1) % philosophers;
    final int right = (philosopher + 0) % philosophers;
    System.out.println(
        "Philosopher "
            + philosopher
            + " wants to eat! Try to pick "
            + left
            + " and "
            + right
            + " forks.");
    while (true) { // philosopher should eat and think alternatively.
      if (forks[left].tryLock(1, TimeUnit.MILLISECONDS)) {
        if (forks[right].tryLock(1, TimeUnit.MILLISECONDS)) { // wait is required to avoid deadlock
          pickLeftFork.run();
          pickRightFork.run();
          eat.run();
          putRightFork.run();
          putLeftFork.run();
          System.out.println("Philosopher " + philosopher + " completes eating.");
          forks[right].unlock();
          forks[left].unlock();
          break;
        } else {
          forks[left].unlock();
        }
      }
    }
  }
}
