package com.qa.business.service;

import static org.junit.Assert.*;

import org.junit.Test;

import com.qa.persistence.domain.Account;

public class ServiceTests {
	private AccountService service = new AccountServiceImplementation();
	private Account account = new Account("Ian", "Mallinson", "9999");

	@Test
	public void addAccount() {
		assertEquals("{\"message\": \"account is blocked\"}", service.createAccount(account));
	}
}