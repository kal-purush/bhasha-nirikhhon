package com.example.tanmay.shoppingapp.DataSet;

public class Product {

    private int product_name;
    private int product_price;
    private int product_thumbnail;
    private int product_image;
    private int product_description;

    public Product(int product_name, int product_price, int product_thumbnail, int product_image, int product_description) {

        this.product_name = product_name;
        this.product_name = product_name;
        this.product_name = product_name;
        this.product_name = product_name;
        this.product_name = product_name;

    }

    public int getProduct_name() {
        return product_name;
    }

    public void setProduct_name(int product_name) {
        this.product_name = product_name;
    }

    public int getProduct_price() {
        return product_price;
    }

    public void setProduct_price(int product_price) {
        this.product_price = product_price;
    }

    public int getProduct_thumbnail() {
        return product_thumbnail;
    }

    public void setProduct_thumbnail(int product_thumbnail) {
        this.product_thumbnail = product_thumbnail;
    }

    public int getProduct_image() {
        return product_image;
    }

    public void setProduct_image(int product_image) {
        this.product_image = product_image;
    }

    public int getProduct_description() {
        return product_description;
    }

    public void setProduct_description(int product_description) {
        this.product_description = product_description;
    }

}