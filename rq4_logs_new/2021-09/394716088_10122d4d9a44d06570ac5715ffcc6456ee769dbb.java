package Entities;

public class Product {
	
	String name;
	Double price;
	Integer quantity;
	
	public Product(String name, Double price, Integer quantity) {
		this.name = name;
		this.price = price;
		this.quantity = quantity;
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
		this.price = price;
	}

	public Integer getQuantity() {
		return quantity;
	}

	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}
	
	public Double totalValue() {
		return this.price * this.quantity;
	}

	public String[] lista() {
		return new String[] {String.format("%s, %.2f", name, this.totalValue())};
	}
	
	
	
}