package com.github.thanglequoc.FruitVendingMachine;

import com.github.thanglequoc.FruitVendingMachine.fruits.Fruit;

public class OptionalFruitVendingMachine {
    
    private static final int MAX_MACHINE_SLOTS = 10;
    
    private Fruit[] fruits;
    
    public OptionalFruitVendingMachine() {
        fruits = new Fruit[MAX_MACHINE_SLOTS];
    }
    
    public void setFruit(int i, Fruit fruit) {
        if (i >= MAX_MACHINE_SLOTS) throw new IllegalArgumentException("Illegal Fruit Slot");
        fruits[i] = fruit;
    }
    
}