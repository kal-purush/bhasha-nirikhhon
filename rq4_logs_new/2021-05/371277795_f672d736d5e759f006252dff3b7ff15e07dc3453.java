package com.jiangls.collection;

import cn.hutool.core.collection.CollUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 对2个ArrayList集合求交集
 */
public class Intersection {

    private static void retainAll(Collection a, Collection b) {
        System.out.println("求交集成功：" + a.retainAll(b));
    }

    private static <T> Collection<T> intersection(Collection<T> a, Collection<T> b) {
        return CollUtil.intersection(a, b);
    }

    public static void main(String[] args) {
        Collection<String> ca = new MyArraylist<>();
        Collection<String> cb = new MyArraylist<>();
        Collection<String> intersectionCollection = null;
        List<Thread> threadList = new ArrayList<>(20);

        // 使用多线程生成集合元素
        for(int i = 0; i < 10; i++) {
            threadList.add(new MyThread(i, ca, "coll"));
            threadList.add(new MyThread(i+8, cb, "coll"));
        }
        // 线程start
        threadList.forEach(t -> t.start());
        // main线程在所有生成元素线程之后运行
        threadList.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
//        for(int i = 0; i < 100000; i++) {
//            ca.add("coll" + i);
//        }
//        for(int i = 80000; i < 180000; i++) {
//            cb.add("coll" + i);
//        }

        Long starTime = System.currentTimeMillis();

        // 使用intersection求2个集合（10万数据量，2万交集数据）交集，需要220ms左右
        intersectionCollection = Intersection.intersection(ca, cb);

        // 使用retainAll求2个集合（10万数据量，2万交集数据）交集，需要55203ms左右
//        retainAll(ca, cb);
        Long endTime = System.currentTimeMillis();
        System.out.println("duration: " + (endTime - starTime));
        System.out.println(intersectionCollection.size());

    }
}

class MyArraylist<E> extends ArrayList {
    final ReentrantLock lock = new ReentrantLock();

    /**
     * 使用ReentrantLock保证add方法线程安全
     * @param o
     * @return
     */
    @Override
    public boolean add(Object o) {
        lock.lock();
        try {
            return super.add(o);
        } finally {
            lock.unlock();
        }
    }
}

class MyThread<E> extends Thread {

    private Integer initialSize;
    private Collection<E> collection;
    private E e;

    public MyThread(Integer initialSize, Collection<E> collection, E e) {
        this.initialSize = initialSize;
        this.collection = collection;
        this.e = e;
    }

    @Override
    public void run() {
        for(Integer i = initialSize*10000; i < initialSize*10000 + 10000; i++) {
            collection.add((E)(e.toString() + i.intValue()));
        }
    }
}