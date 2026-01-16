package com.codecool.shop.model;


public class Order {

    private String name;
    private String email;
    private Integer phoneNumber;
    private String billingAddress; //country,city,zipcode,address
    private String shippingAddress;
    private ShoppingCart cart;
    private static Order instance = null;


    public void addPersonalInfo(String name, String email, Integer phoneNumber, String billingAddress, String shippingAddress){
        this.name=name;
        this.email=email;
        this.phoneNumber=phoneNumber;
        this.billingAddress=billingAddress;
        this.shippingAddress=shippingAddress;
    }

    protected Order(ShoppingCart cart){
        this.cart=cart;
    }


    public static Order getInstance(ShoppingCart cart) {
        if (instance == null) {
            instance = new Order(cart);
        }
        return instance;
    }

}