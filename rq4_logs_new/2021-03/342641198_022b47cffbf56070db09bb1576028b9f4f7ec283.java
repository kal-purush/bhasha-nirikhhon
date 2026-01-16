package com.banking.bankservice.controller;

import com.banking.bankservice.mapper.AccountMapper;
import com.banking.bankservice.mapper.TransactionMapper;
import com.banking.bankservice.model.Account;
import com.banking.bankservice.model.dto.request.AccountRequestDto;
import com.banking.bankservice.model.dto.request.TransactionRequestDto;
import com.banking.bankservice.model.dto.response.AccountResponseDto;
import com.banking.bankservice.model.dto.response.TransactionResponseDto;
import com.banking.bankservice.service.AccountService;
import com.banking.bankservice.service.TransactionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/accounts")
public class AccountController {
    private final AccountService accountService;
    private final AccountMapper accountMapper;
    private final TransactionService transactionService;
    private final TransactionMapper transactionMapper;

    @Autowired
    public AccountController(AccountService accountService, AccountMapper accountMapper,
                             TransactionService transactionService,
                             TransactionMapper transactionMapper) {
        this.accountService = accountService;
        this.accountMapper = accountMapper;
        this.transactionService = transactionService;
        this.transactionMapper = transactionMapper;
    }

    @PostMapping
    public void create(@RequestBody AccountRequestDto accountRequestDto) {
        Account account = accountMapper.fromDto(accountRequestDto);
        account.setActive(true);
        accountService.save(account);
    }

    @GetMapping("/by-phone")
    public List<AccountResponseDto> getAllByPhoneNumber(@RequestParam String phoneNumber) {
        return accountService.findAccountsByPhoneNumber(phoneNumber).stream()
                .map(accountMapper::toDto)
                .collect(Collectors.toList());
    }

    @PostMapping("/transfer")
    public void transfer(@RequestBody TransactionRequestDto transactionRequestDto) {
        Account accountFrom = accountService
                .getAccountByAccountNumber(transactionRequestDto.getAccountFrom());
        Account accountTo = accountService
                .getAccountByAccountNumber(transactionRequestDto.getAccountTo());
        Double amount = transactionRequestDto.getAmount();
        transactionService.transfer(accountFrom, accountTo, amount);
    }

    @GetMapping("/{accountNumber}")
    public Double getBalanceByAccountNumber(@PathVariable String accountNumber) {
        return accountService.getAccountByAccountNumber(accountNumber).getBalance();
    }

    @GetMapping("/history/{accountNumber}")
    public List<TransactionResponseDto> getHistoryByAccountNumber
            (@PathVariable String accountNumber, @RequestParam(defaultValue = "1") Integer page,
             @RequestParam(defaultValue = "10") Integer size) {
        return transactionService.getAllByAccount(page, size, accountService
                .getAccountByAccountNumber(accountNumber)).stream()
                .map(transactionMapper::toDto)
                .collect(Collectors.toList());
    }

    @PatchMapping("/{accountNumber}")
    public void blockAccount(@PathVariable String accountNumber) {
        accountService.blockAccount(accountNumber);
    }
}