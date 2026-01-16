package com.hind.org.design.patterns.simpleExample;

public class ConcreteStrategyA implements Strategy {
    @Override
    public void execute() {
        System.out.println("Execution of Strategy A");
    }
}