package com.se.fit.TravelProject.entities;

import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "Account")
public class Account implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6758543535589076148L;
	@Id
	private String userName;
	@Column(nullable = false)
	private String passWord;
	@Enumerated(EnumType.ORDINAL)
	private ERole eRole;
	@ManyToOne
	@JoinColumn(name = "User_Id")
	private User user;

	public Account() {
		// TODO Auto-generated constructor stub
	}

	public Account(String userName, String passWord, ERole eRole, User user) {
		super();
		this.userName = userName;
		this.passWord = passWord;
		this.eRole = eRole;
		this.user = user;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassWord() {
		return passWord;
	}

	public void setPassWord(String passWord) {
		this.passWord = passWord;
	}

	public ERole geteRole() {
		return eRole;
	}

	public void seteRole(ERole eRole) {
		this.eRole = eRole;
	}

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	@Override
	public String toString() {
		return "Account [userName=" + userName + ", passWord=" + passWord + ", eRole=" + eRole + ", user=" + user + "]";
	}

}