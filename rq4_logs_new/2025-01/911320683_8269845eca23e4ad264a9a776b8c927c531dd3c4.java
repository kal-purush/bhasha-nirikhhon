package com.sofkau.bank.controllers;

import java.util.List;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sofkau.bank.entities.Account;
import com.sofkau.bank.http.responses.TransactionResponse;
import com.sofkau.bank.services.accounts.AccountService;
import com.sofkau.bank.services.transactions.TransactionService;

@RestController
@RequestMapping("/transactions")
public class TransactionController {
    private final TransactionService transactionService;
    private final AccountService accountService;

    public TransactionController(TransactionService transactionService, AccountService accountService) {
        this.transactionService = transactionService;
        this.accountService = accountService;
    }

    @GetMapping("/account/{number}")
    public List<TransactionResponse> findAccountTransactions(@PathVariable("number") UUID accountNumber) {
        Account account = accountService
                .findAccountByNumber(accountNumber);

        return transactionService.findAccountTransactions(account).stream()
                .map(TransactionResponse::from)
                .toList();
    }
}