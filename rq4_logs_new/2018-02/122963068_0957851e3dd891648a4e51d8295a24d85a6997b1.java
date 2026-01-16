package com.qa.domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.validation.constraints.Size;

@Entity
public class Account implements DomainEntity {

	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Id
	@Column(name="idAccount")
	private int id;
	@Size(max=500)
	@Column(name="account_Desc")
	private String description;
	@Size(max=10)
	@Column(name="account_Type")
	private String type;
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="idCustomer")
	private int custoerRef;

	public Account(int accountID, String accountDescription, String accountType, int customerID) {

	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="idCustomer")
	public int getCustoerRef() {
		return custoerRef;
	}

	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="idCustomer")
	public void setCustoerRef(int custoerRef) {
		this.custoerRef = custoerRef;
	}


}