package com.banking.bankservice.service.impl;

import com.banking.bankservice.model.Account;
import com.banking.bankservice.repository.AccountRepository;
import com.banking.bankservice.service.AccountService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AccountServiceImpl implements AccountService {
    private final AccountRepository accountRepository;

    @Autowired
    public AccountServiceImpl(AccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }

    @Override
    public Account save(Account account) {
        return accountRepository.save(account);
    }

    @Override
    public Account getAccountByAccountNumber(String accountNumber) {
        return accountRepository.getAccountByAccountNumber(accountNumber).get();
    }

    @Override
    public List<Account> findAccountsByPhoneNumber(String phoneNumber) {
        return accountRepository.findAllAccountsByPhoneNumber(phoneNumber);
    }

    @Override
    public void blockAccount(String accountNumber) {
        Account accountForBlocking = getAccountByAccountNumber(accountNumber);
        accountForBlocking.setActive(false);
        accountRepository.save(accountForBlocking);
    }
}