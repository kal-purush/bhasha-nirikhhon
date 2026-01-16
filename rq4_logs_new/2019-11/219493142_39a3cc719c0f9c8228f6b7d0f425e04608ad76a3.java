package com.lindeyao.jdk.learnjdk8.design_patterns.template;

/**
 * 模板方法--抽象模型(例子为悍马模型)
 *
 * @author ldy
 * @create 2019-11-21 15:48
 */
public abstract class AbstractHummerModel {

    // 启动引擎
    abstract void start();

    // 关闭引擎
    abstract void stop();

    // 响声
    abstract void alarm();

    // 跑起来
    //public abstract void run();

    // 模板方法
    public final void run() {
        // 所有汽车都是下面的执行顺序
        // 开始启动
        this.start();
        // 机器发出响声
        this.alarm();
        // 到达目的地停止
        this.stop();
    }
}