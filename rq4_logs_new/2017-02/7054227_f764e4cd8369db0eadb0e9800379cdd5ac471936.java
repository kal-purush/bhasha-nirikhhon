package com.byeight.tags;

public interface Collector extends Runnable {
    public void stop();
    public boolean isReady();
}