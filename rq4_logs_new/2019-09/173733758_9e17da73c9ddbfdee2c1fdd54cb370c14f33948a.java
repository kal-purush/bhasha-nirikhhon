package io.github.dlinov.leetcode1k;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

class Task1117BuildingH2O {
    class H2O {

        private final CyclicBarrier hBarrier;
        private final Semaphore hSemaphore;
        private final Semaphore oSemaphore;

        public H2O() {
            hBarrier = new CyclicBarrier(2);
            hSemaphore = new Semaphore(2);
            oSemaphore = new Semaphore(0);
        }

        public void hydrogen(Runnable releaseHydrogen) throws InterruptedException {
            hSemaphore.acquire();
            try {
                hBarrier.await();
            } catch (BrokenBarrierException e) {
            }
            // releaseHydrogen.run() outputs "H". Do not change or remove this line.
            releaseHydrogen.run();
            oSemaphore.release();
        }
    
        public void oxygen(Runnable releaseOxygen) throws InterruptedException {
            oSemaphore.acquire(2);
            // releaseOxygen.run() outputs "O". Do not change or remove this line.
            releaseOxygen.run();
            hSemaphore.release(2);
        }
    }
}