package edu.cnm.deepdive.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class AtomicRaceCondition2 {
  

  private static final int UPPER_LIMIT = 20_000_000;
  private static final int NUM_THREADS = 10;
  private static final String START_MESSAGE = "Starting %d threads.%n";
  private static final String THREAD_COMPLETE_MESSAGE = "Thread %d complete.%n";
  private static final String FINISH_MESSAGE = "Sum = %, d.%n";
  
  private AtomicLong sum;
  private CountDownLatch signal;
  
  public static void main(String[] args) {
    AtomicRaceCondition2 race = new AtomicRaceCondition2();
    race.start();
    race.finish();

  }
  
  private void start() {
    System.out.printf(START_MESSAGE, NUM_THREADS);
    signal= new CountDownLatch(NUM_THREADS);
    sum = new AtomicLong(0);
    for (int i = 0; i < NUM_THREADS; i++) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          for (int i = 0; i < UPPER_LIMIT; i++) {
            sum.addAndGet(ThreadLocalRandom.current().nextInt(2));
          }
          signal.countDown();
        }
        
      }).start();
    }
  }
  private void finish() {
    try {
      signal.await();
    } catch (InterruptedException ex) {
      //Do nothing
    } finally {
      System.out.printf(FINISH_MESSAGE, sum.longValue());
    }
  }

}