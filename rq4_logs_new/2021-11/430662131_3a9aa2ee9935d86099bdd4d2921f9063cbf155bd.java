package com.tw.d05_reflex;

public class LazyMan {

    private LazyMan() {
        System.out.println(Thread.currentThread().getName() + "-ok");
    }

    private volatile static LazyMan lazyMan;

    public static LazyMan getInstance() {
        synchronized (LazyMan.class) {
            if (lazyMan == null) {
                lazyMan = new LazyMan();
            }
        }
        return lazyMan;
    }
}