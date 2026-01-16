package com.qa.domain;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.validation.constraints.Size;

public class Customer implements DomainEntity {

	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Id
	@Column(name = "idCustomer")
	private Long id;

	@Size(max = 45)
	@Column(name = "CustName")
	private String customerName;

	@Size(max = 45)
	@Column(name = "UserName")
	private String userName;

	@Size(max = 256)
	@Column(name = "Email")
	private String email;

	@Size(max = 256)
	@Column(name = "password")
	private String password;

	public Customer(String customerName, String userName, String email, String password) {
		this.customerName = customerName;
		this.userName = userName;
		this.email = email;
		this.password = password;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getCustomerName() {
		return customerName;
	}

	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}