package com.se.fit.TravelProject.entities;

import org.springframework.validation.annotation.Validated;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

@Validated
public class UserAccount {
	@Valid
	private User user;
	@Valid
	private Account account;

	public UserAccount() {
		// TODO Auto-generated constructor stub
	}

	public UserAccount(User user, Account account) {
		super();
		this.user = user;
		this.account = account;
	}

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public Account getAccount() {
		return account;
	}

	public void setAccount(Account account) {
		this.account = account;
	}

	@Override
	public String toString() {
		return "UserAccount [user=" + user + ", account=" + account + "]";
	}

}