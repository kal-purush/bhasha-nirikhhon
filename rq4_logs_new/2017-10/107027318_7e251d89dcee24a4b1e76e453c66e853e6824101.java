package com.itau.mygod.product;

import java.io.File;

import cn.bmob.v3.BmobObject;

public class Product extends BmobObject{
	private File image;
	private String title;
	private String price;
	public File getImage() {
		return image;
	}
	public void setImage(File image) {
		this.image = image;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getPrice() {
		return price;
	}
	public void setPrice(String price) {
		this.price = price;
	}

}