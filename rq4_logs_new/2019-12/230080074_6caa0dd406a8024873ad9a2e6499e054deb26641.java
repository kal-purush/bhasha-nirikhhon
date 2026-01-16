package com.lanxin.bean;

import java.io.Serializable;

public class Product implements Serializable{
    private Integer id;
    private String name;
    private Double price;
    private String categrory;
    private Integer num;
    private String imgurl;
    private String description; 
    public Integer getId() {
        return id;
    }
    public void setId(Integer id) {
        this.id = id;
    } 
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    } 
    public Double getPrice() {
    	return price;
    }
    public void setPrice(Double price) {
    	this.price=price;
    }
    public String getCategrory() {
    	return categrory;
    }
    public void setCategrory(String categrory) {
    	this.categrory=categrory;
    }
    public Integer getNum() {
    	return num;
    }
    public void setNum(Integer num) {
    	this.num=num;
    }
    public String getImgurl() {
    	return imgurl;
    }
    public void setImgurl(String imgurl) {
    	this.imgurl=imgurl;
    }
    public String getDescription() {
    	return description;
    }
    public void setDescription(String description) {
    	this.description=description;
    }
}