package com.zing.mode.factory;

/**
 * @author zing
 * @date 2018/3/9 10:41
 */
public abstract class Product {
    public String name;

    public Product(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}