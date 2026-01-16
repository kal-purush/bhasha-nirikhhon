package com.example.nermingraca.bitbayapp;

/**
 * Created by nermingraca on 14/04/15.
 */
public class Product {

    private int mId;
    private String mName;
    private double mPrice;

    public Product(int mId, String mName, double mPrice) {
        this.mId = mId;
        this.mName = mName;
        this.mPrice = mPrice;
    }

    public int getmId() {
        return mId;
    }

    public void setmId(int mId) {
        this.mId = mId;
    }

    public String getmName() {
        return mName;
    }

    public void setmName(String mName) {
        this.mName = mName;
    }

    public double getmPrice() {
        return mPrice;
    }

    public void setmPrice(double mPrice) {
        this.mPrice = mPrice;
    }
}