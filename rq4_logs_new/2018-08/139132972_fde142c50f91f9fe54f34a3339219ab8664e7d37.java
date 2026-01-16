package com.mouse.autumn.chapter04.castest;

/**
 * Created by Mahone Wu on 2018/8/15.
 */
public class SyncThread implements Runnable {

    protected String name;

    protected long starttime;

    TestAtomic out;

    public SyncThread(long starttime, TestAtomic out) {
        this.starttime = starttime;
        this.out = out;
    }

    @Override
    public void run() {
        int v = out.inc();

    }
}