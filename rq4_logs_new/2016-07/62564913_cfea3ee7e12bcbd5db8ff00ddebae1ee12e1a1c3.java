package pl.workshop.lib;

import java.math.BigDecimal;
import java.util.Date;

public class Product {

	private int id;
	private String code;
	private String name;
	private String description;
	private BigDecimal price;
	private Date purchaseDate;

	public Product() {
	}

	public Product(int id, String code, String name, BigDecimal price, Date purchaseDate) {
		super();
		this.id = id;
		this.code = code;
		this.name = name;
		this.price = price;
		this.purchaseDate = purchaseDate;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
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

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal price) {
		this.price = price;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Date getPurchaseDate() {
		return purchaseDate;
	}

	public void setPurchaseDate(Date purchaseDate) {
		this.purchaseDate = purchaseDate;
	}

}