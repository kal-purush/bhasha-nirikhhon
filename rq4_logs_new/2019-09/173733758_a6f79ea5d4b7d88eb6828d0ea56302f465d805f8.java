package io.github.dlinov.leetcode1k;

import java.util.concurrent.Semaphore;
import java.util.function.IntConsumer;

class Task1116PrintZeroEvenOdd {
    class ZeroEvenOdd {
        private final int n;
        private final Semaphore s1 = new Semaphore(1);
        private final Semaphore s2 = new Semaphore(0);
        private final Semaphore s3 = new Semaphore(1);
        
        public ZeroEvenOdd(int n) {
            this.n = n;
        }

        // printNumber.accept(x) outputs "x", where x is an integer.
        public void zero(IntConsumer printNumber) throws InterruptedException {
            for (int i = 1; i <= n; ++i) {
                s1.acquire(1);
                printNumber.accept(0);
                s2.release(1);
                s3.release(1);
            }
        }

        public void even(IntConsumer printNumber) throws InterruptedException {
            for (int i = 2; i <= n; i += 2) {
                s2.acquire(2);
                printNumber.accept(i);
                s1.release(1);
            }
        }

        public void odd(IntConsumer printNumber) throws InterruptedException {
            for (int i = 1; i <= n; i += 2) {
                s3.acquire(2);
                printNumber.accept(i);
                s1.release(1);
            }
        }
    }
}