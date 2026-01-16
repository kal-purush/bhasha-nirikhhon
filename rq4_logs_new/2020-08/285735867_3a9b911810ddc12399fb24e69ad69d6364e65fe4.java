package com.cc.pos.product.bean;

import java.util.List;

import lombok.Data;

@Data
public class Product {
	
	private String id;
	private String name;
	private String description;
	private boolean active;
	private String image;
	List<ProductLineItem> productList;
	
	/**
	 * 
	 */
	public Product() {
		super();
	}
	/**
	 * @param id
	 * @param name
	 * @param description
	 * @param active
	 * @param image
	 * @param productList
	 */
	public Product(String id, String name, String description, boolean active, String image,
			List<ProductLineItem> productList) {
		super();
		this.id = id;
		this.name = name;
		this.description = description;
		this.active = active;
		this.image = image;
		this.productList = productList;
	}


	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
	}
	public String getImage() {
		return image;
	}
	public void setImage(String image) {
		this.image = image;
	}
	public List<ProductLineItem> getProductList() {
		return productList;
	}
	public void setProductList(List<ProductLineItem> productList) {
		this.productList = productList;
	}
	@Override
	public String toString() {
		return "Product [id=" + id + ", name=" + name + ", description=" + description + ", active=" + active
				+ ", image=" + image + ", productList=" + productList + "]";
	}
		
}