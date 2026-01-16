package com.tlyong1992.client.thread;

import com.tlyong1992.client.model.BaseObject;

import java.util.List;

/**
 * 事件处理线程
 * USER：tangly
 * DATE：2017/5/22
 * TIME：16:16
 */
public class EventThread implements Runnable {

    List<BaseObject> objectList;

    public EventThread(List<BaseObject> objectList) {
        this.objectList = objectList;
    }

    @Override
    public void run() {
        while (true) {
            handleMove();
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 处理对象的移动
     */
    private void handleMove() {
        for (BaseObject moveObject : objectList) {
            moveObject.move();
        }
    }
}