package fr.training.spring.shop.application.customer;

import java.io.Serializable;

public class CustomerDTO implements Serializable {

	private static final long serialVersionUID = 1L;

	private String customerID;

	private String name;

	private String password;

	public CustomerDTO() {
		super();
	}

	public CustomerDTO(String customerID, String name, String password) {
		this.customerID = customerID;
		this.name = name;
		this.password = password;
	}

	public CustomerDTO(String name, String password) {
		super();
		this.name = name;
		this.password = password;
	}

	public String getCustomerID() {
		return customerID;
	}

	public void setCustomerID(String customerID) {
		this.customerID = customerID;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}