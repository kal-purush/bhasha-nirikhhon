package com.dpp.os.semaphore;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @program: java-algorithms
 * @description: 复杂的生产者消费者问题
 * 桌上有一只盘子，每次只能放入一只水果。爸爸专向盘子中放苹果（apple），妈妈专向盘子中放桔子（orange），一个儿子专等吃盘子中的桔子，一个女儿专等吃盘子中的苹果
 * @author: duanpp
 * @create: 2018-12-05 11:05
 **/
public class ComplexProducerConsumer {

    /**
     * 水果盘
     */
    private static class FruitPlate{
        /**
         * 存放的水果
         */
        private Fruit fruit;
        /**
         * 盘子只能放入一个fruit
         */
        private Semaphore mutex = new Semaphore(1);
        /**
         * 苹果信号量
         */
        private Semaphore apple = new Semaphore(0);
        /**
         * 橘子信号量
         */
        private Semaphore orange = new Semaphore(0);

        public Fruit getFruit() {
            Fruit f = this.fruit;
            this.fruit = null;
            return f;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("FruitPlate{");
            sb.append("fruit=").append(fruit);
            sb.append(", mutex=").append(mutex);
            sb.append(", apple=").append(apple);
            sb.append(", orange=").append(orange);
            sb.append('}');
            return sb.toString();
        }
    }

    private interface Fruit{
        String fruitName();
    }

    private static class Apple implements Fruit{
        @Override
        public String fruitName() {
            return "apple";
        }
    }

    private static class Orange implements Fruit{
        @Override
        public String fruitName() {
            return "orange";
        }
    }

    /**
     * 爸爸
     */
    private static class Father implements Runnable{

        private FruitPlate buffer;

        public Father(FruitPlate buffer){
            this.buffer = buffer;
        }

        @Override
        public void run() {
            try {
                buffer.mutex.acquire();
                Apple apple = new Apple();
                buffer.fruit = apple;
                TimeUnit.MILLISECONDS.sleep(200);
                buffer.apple.release();
                System.out.println("爸爸放入了一个:"+apple.fruitName());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 妈妈
     */
    private static class Mather implements Runnable{

        private FruitPlate buffer;

        public Mather(FruitPlate buffer){
            this.buffer = buffer;
        }

        @Override
        public void run() {
            try {
                buffer.mutex.acquire();
                Orange orange = new Orange();
                buffer.fruit = orange;
                TimeUnit.MILLISECONDS.sleep(200);
                buffer.orange.release();
                System.out.println("妈妈放入了一个:"+orange.fruitName());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 儿子
     */
    private static class Son implements Runnable{

        private FruitPlate buffer;

        public Son(FruitPlate buffer){
            this.buffer = buffer;
        }

        @Override
        public void run() {
            try {
                buffer.orange.acquire();
                Fruit fruit = buffer.getFruit();
                TimeUnit.MILLISECONDS.sleep(300);
                System.out.println("儿子吃了一个:"+fruit.fruitName());
                buffer.mutex.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 女儿
     */
    private static class Girl implements Runnable{

        private FruitPlate buffer;

        public Girl(FruitPlate buffer){
            this.buffer = buffer;
        }

        @Override
        public void run() {
            try {
                buffer.apple.acquire();
                Fruit fruit = buffer.getFruit();
                TimeUnit.MILLISECONDS.sleep(300);
                System.out.println("女儿吃了一个:"+fruit.fruitName());
                buffer.mutex.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        FruitPlate fruitPlate = new FruitPlate();
        for (int i = 0; i < 3; i++) {
            Thread father = new Thread(new Father(fruitPlate));
            father.start();
        }
        for (int i = 0; i < 3; i++) {
            Thread mather = new Thread(new Mather(fruitPlate));
            mather.start();
        }
        for (int i = 0; i < 3; i++) {
            Thread son = new Thread(new Son(fruitPlate));
            son.start();
        }
        for (int i = 0; i < 3; i++) {
            Thread girl = new Thread(new Girl(fruitPlate));
            girl.start();
        }
    }
}