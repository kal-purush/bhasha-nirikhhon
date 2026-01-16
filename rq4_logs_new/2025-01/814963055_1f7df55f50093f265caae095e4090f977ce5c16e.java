package com.example.multithreadingProject.CoffeeShop.CoffeeDecorator;

import com.example.multithreadingProject.CoffeeShop.Coffee.Coffee;

public class MilkDecorator extends CoffeeDecorator{
    private final Coffee coffee;

    public MilkDecorator(Coffee coffee) {
        this.coffee = coffee;
    }

    @Override
    public String getDescription() {
        return coffee.getDescription() + " + milk";
    }

    @Override
    public Double getCost() {
        return coffee.getCost() + 3.00;
    }
}