package com.banking.bankservice.mapper.impl;

import com.banking.bankservice.mapper.AccountMapper;
import com.banking.bankservice.model.Account;
import com.banking.bankservice.model.Currency;
import com.banking.bankservice.model.dto.request.AccountRequestDto;
import com.banking.bankservice.model.dto.response.AccountResponseDto;
import org.springframework.stereotype.Component;

@Component
public class AccountMapperImpl implements AccountMapper {
    @Override
    public Account fromDto(AccountRequestDto accountRequestDto) {
        Account account = new Account();
        account.setAccountNumber(accountRequestDto.getAccountNumber());
        account.setBalance(accountRequestDto.getBalance());
        account.setActive(true);
        account.setCurrencyType(Currency.valueOf(accountRequestDto.getCurrencyType().toString()));
        return  account;
    }

    @Override
    public AccountResponseDto toDto(Account account) {
        return AccountResponseDto.builder()
                .accountNumber(account.getAccountNumber())
                .balance(account.getBalance())
                .currency(account.getCurrencyType().toString())
                .build();
    }
}