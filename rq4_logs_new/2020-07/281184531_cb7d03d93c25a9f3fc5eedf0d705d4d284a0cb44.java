package com.smbcgroup.training.atm;

import java.math.BigDecimal;

import com.smbcgroup.training.atm.ATMServiceException.Type;
import com.smbcgroup.training.atm.dao.AccountDAO;
import com.smbcgroup.training.atm.dao.AccountNotFoundException;
import com.smbcgroup.training.atm.dao.UserNotFoundException;

public class ATMService {

	private AccountDAO dao;

	public ATMService(AccountDAO dao) {
		this.dao = dao;
	}

	public String[] getUserAccounts(String userId) throws UserNotFoundException {
		return dao.getUserAccounts(userId);
	}

	public BigDecimal getBalance(String accountNumber) throws AccountNotFoundException {
		return dao.getAccountBalance(accountNumber);
	}

	public void deposit(String accountNumber, BigDecimal amount) throws AccountNotFoundException, ATMServiceException {
		BigDecimal currentBalance = getBalance(accountNumber);
		validateIsPositiveAmount(amount);
		dao.updateAccountBalance(accountNumber, currentBalance.add(amount));
	}

	public void withdraw(String accountNumber, BigDecimal amount) throws AccountNotFoundException, ATMServiceException {
		BigDecimal currentBalance = getBalance(accountNumber);
		validateIsPositiveAmount(amount);
		if (!wouldLeaveAtLeastMinimumBalance(currentBalance, amount))
			throw new ATMServiceException(Type.INSUFFICIENT_FUNDS);
		dao.updateAccountBalance(accountNumber, currentBalance.subtract(amount));
	}

	private void validateIsPositiveAmount(BigDecimal amount) throws ATMServiceException {
		if (BigDecimal.ZERO.compareTo(amount) > 0)
			throw new ATMServiceException(Type.NON_POSITIVE_AMOUNT);
	}

	private boolean wouldLeaveAtLeastMinimumBalance(BigDecimal balance, BigDecimal withdrawalAmount) {
		return balance.subtract(withdrawalAmount).compareTo(BigDecimal.TEN) >= 0;
	}

}