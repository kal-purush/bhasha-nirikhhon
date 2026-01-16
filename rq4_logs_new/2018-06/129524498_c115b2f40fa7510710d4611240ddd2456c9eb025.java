package com.github.thanglequoc.FruitVendingMachine;

import com.github.thanglequoc.FruitVendingMachine.fruits.Fruit;

public class FruitOutOfStockException extends RuntimeException {
    
    @Override
    public String getMessage() {
        return "This fruit is out of stocks";
    }

}