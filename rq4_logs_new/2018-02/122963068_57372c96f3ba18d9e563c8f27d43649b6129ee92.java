package com.qa.domain;

import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.validation.constraints.Size;

public class Customer implements DomainEntity {

	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Id
	@Column(name="idCustomer")
	private Long id;
	@Size(max=45)
	@Column(name="custName")
	private String customerName;
	@Size(max=500)
	@Column(name="account_Desc")
	private String description;
	@Size(max=10)
	@Column(name="account_Type")
	private String type;
	@ManyToOne(fetch=FetchType.EAGER)
	@JoinColumn(name="idCustomer")
	private int customerRef;
	
}