package com.cicdproject.BankPortal.service;

import java.util.List;

import com.cicdproject.BankPortal.dao.BankAccountDAOInterface;
import com.cicdproject.BankPortal.model.BankAccountInterface;

public class BankAccountServiceImpl implements BankAccountServiceInterface {
    private BankAccountDAOInterface bankAccountDAOInterface;
	 
	@Override
	public void create(BankAccountInterface newAccount) {
		bankAccountDAOInterface.create(newAccount);
	}

	@Override
	public void update(BankAccountInterface existingAccount) {
		bankAccountDAOInterface.update(existingAccount);
	}

	@Override
	public void delete(String email) {
		bankAccountDAOInterface.delete(email);
	}

	@Override
	public String fetchAccountByEMail(String email) {
		BankAccountInterface accountReference = bankAccountDAOInterface.getUserAccountByUserName(email);
		if (accountReference != null) {
			return accountReference.getPassword();
		}
		
		return "No Account with email: " + email;
	}

	
}