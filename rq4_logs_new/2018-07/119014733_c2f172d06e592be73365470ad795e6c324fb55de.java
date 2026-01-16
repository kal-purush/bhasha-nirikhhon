package com.shejimoshi.proxy.one.two;

public class Client {
    public static void main(String[] args){
        //Subject subject=new RealSubject1();
        Subject subject = (Subject) new ProxyHandler().newProxyHandler(new RealSubject1());
        subject.operate1();
       //Subject subject= new ProxyHandler().newProxyHandler(new RealSubject1());
       // Subject
    }
}