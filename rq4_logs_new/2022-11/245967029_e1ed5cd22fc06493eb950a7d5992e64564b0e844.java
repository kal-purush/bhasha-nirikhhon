package com.lishan.ssm.utils.designPatterns.observer;

public class Client {
    public static void main(String[] args){
        //创建一个主题
        Subject sub = new ConcreteSubject();
        ConcreteObserver1 concreteObserver1 = new ConcreteObserver1();
        ConcreteObserver2 concreteObserver2 = new ConcreteObserver2();
        //往主题中添加一个观察者
        sub.addObserver(concreteObserver1); //添加观察者1
        sub.addObserver(concreteObserver2); //添加观察者2
        //开始活动,该方法会执行主题中的所有观察者
        sub.doSomething();

        sub.removeObserver(concreteObserver1);
        sub.doSomething();
    }
}