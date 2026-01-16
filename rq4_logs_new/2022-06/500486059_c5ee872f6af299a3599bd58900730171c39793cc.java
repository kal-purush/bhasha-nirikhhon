package com.example.telecom.controller;

import com.example.telecom.model.Account_Block.Account;
import com.example.telecom.repository.AccountRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class AccountController {

    @Autowired
    AccountRepository accountRepository;



    @GetMapping("accounts")
    public List<Account> showAllAccounts() { return accountRepository.findAll(); }



    @GetMapping("accounts-number/")
    @ResponseStatus(HttpStatus.OK)
    public Account findAccountByNumber(@RequestParam String number){
        return accountRepository.findAccountByPrimaryPhoneNumber(number);
    }

    @GetMapping("accounts-status/")
    @ResponseStatus(HttpStatus.OK)
    public List<Object> findNumberOfAccountsByStatus(@RequestParam String status){
        return accountRepository.findCountofAccountsByStatus(status);
    }


}