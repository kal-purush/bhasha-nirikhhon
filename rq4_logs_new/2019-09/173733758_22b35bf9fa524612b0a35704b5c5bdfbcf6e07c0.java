package io.github.dlinov.leetcode1k;

import java.util.concurrent.*;

class Task1115PrintFooBarAlternately {
    class FooBar {
        private int n;

        private final Semaphore sem = new Semaphore(1, true);
        private final Semaphore sem2 = new Semaphore(0, true);

        public FooBar(int n) {
            this.n = n;
        }

        public void foo(Runnable printFoo) throws InterruptedException {
            
            for (int i = 0; i < n; i++) {
                sem.acquire();
                // printFoo.run() outputs "foo". Do not change or remove this line.
                printFoo.run();
                sem2.release();
            }
        }

        public void bar(Runnable printBar) throws InterruptedException {
            
            for (int i = 0; i < n; i++) {
                sem2.acquire();
                // printBar.run() outputs "bar". Do not change or remove this line.
                printBar.run();
                sem.release();
            }
        }
    }
}