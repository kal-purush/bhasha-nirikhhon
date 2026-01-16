package com.cicdproject.BankPortal.model;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.persistence.Column;

@Entity
@Table(name = "BankUserAccounts")
public class BankAccountModelImpl implements BankAccountInterface{
	
	
	@Id
	@Column(name="UserEmail")
	@NotNull
	@Size(max=200)
	private java.lang.String userEmail;
	
	@Column(name="UserPassword")
	@NotNull
	@Size(max=60)
	private java.lang.String userPassword;
	
	public BankAccountModelImpl()
	{
		userEmail = "";
		userPassword = "";
	}
	
	public BankAccountModelImpl(String userEmail, String userPassword) {
		this.userEmail = userEmail;
		this.userPassword = userPassword;
	}

	public String getEmail()
	{
		return userEmail;	
	}

	public void setEmail(String email)
	{
		this.userEmail = email;
	}
	
	public String getPassword()
	{
		return userPassword;
		
	}
	
	public void setPassword(String password)
	{
		this.userPassword = password;
	}
	
	@Override
	public String toString() {
		return "BankAccountModelImpl [userEmail=" + userEmail + ", userPassword=" + userPassword + "]";
	}
}