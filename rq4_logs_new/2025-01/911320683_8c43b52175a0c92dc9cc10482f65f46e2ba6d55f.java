package com.sofkau.bank.controllers;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sofkau.bank.entities.Account;
import com.sofkau.bank.entities.Client;
import com.sofkau.bank.entities.User;
import com.sofkau.bank.entities.Account.Type;
import com.sofkau.bank.http.requests.CreateAccountRequest;
import com.sofkau.bank.http.responses.AccountResponse;
import com.sofkau.bank.services.accounts.AccountService;

@RestController
@RequestMapping("/accounts")
public class AccountController {
    private final AccountService accountService;

    public AccountController(AccountService accountService) {
        this.accountService = accountService;
    }

    @PostMapping
    public AccountResponse createAccount(CreateAccountRequest accountRequest) {
        Type type = accountService
                .findAccountTypeByName(accountRequest.type());

        Client client = Client.from(User.builder().build()).build(); // TODO: should be logged in client

        Account account = accountRequest.toAccount(type, client);

        return AccountResponse
                .from(accountService.createAccount(account));
    }

    @GetMapping
    public List<AccountResponse> findClientAccounts() {
        Client client = Client.from(User.builder().build()).build(); // TODO: should be logged in client

        return accountService.findClientAccounts(client).stream()
                .map(AccountResponse::from)
                .toList();
    }
}