package io.github.dlinov.leetcode1k;

import java.util.concurrent.*;

class Task1114PrintInOrder {
    class Foo {

        private final CountDownLatch cd1 = new CountDownLatch(1);
        private final CountDownLatch cd2 = new CountDownLatch(1);

        public Foo() {
            
        }

        public void first(Runnable printFirst) throws InterruptedException {
            
            // printFirst.run() outputs "first". Do not change or remove this line.
            printFirst.run();
            cd1.countDown();
        }

        public void second(Runnable printSecond) throws InterruptedException {
            cd1.await();
            // printSecond.run() outputs "second". Do not change or remove this line.
            printSecond.run();
            cd2.countDown();
        }

        public void third(Runnable printThird) throws InterruptedException {
            cd2.await();
            // printThird.run() outputs "third". Do not change or remove this line.
            printThird.run();
        }
    }
}