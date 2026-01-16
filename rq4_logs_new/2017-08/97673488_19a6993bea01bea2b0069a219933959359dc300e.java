package com.example.shanig.doctorapp;

/**
 * Created by ShaniG on 8/10/2017.
 */

public class Categories {

    private String categoryName,categoryImage;
    public Categories() {
    }

    public Categories(String categoryName) {
        this.categoryName = categoryName;
    }

    public Categories(String categoryName, String categoryImage) {
        this.categoryName = categoryName;
        this.categoryImage = categoryImage;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public String getCategoryImage() {
        return categoryImage;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public void setCategoryImage(String categoryImage) {
        this.categoryImage = categoryImage;
    }
}