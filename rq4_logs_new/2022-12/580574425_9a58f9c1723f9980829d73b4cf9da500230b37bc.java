package com.pandora.backend.controller;

import com.pandora.backend.domain.Accounts;
import com.pandora.backend.service.AccountsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;


@RestController
@RequestMapping("accounts")
public class AccountsController {
    @Autowired
    private AccountsService service;

    @GetMapping
    public List<Accounts> getAll() {
        return this.service.findAll();
    }

    @GetMapping("/id")
    public Accounts get(@PathVariable Integer id) {
        return this.service.findById(id);
    }

    @PostMapping
    public Accounts post(@RequestBody Accounts account) {
        return this.service.save(account);
    }

    @PutMapping
    public Accounts put(@PathVariable Integer id,
                        @RequestBody Accounts account) {
        return this.service.save(account);
    }

    @DeleteMapping
    public void softDelete(@PathVariable Integer id) {
        this.service.softDelete(id);
    }

    @DeleteMapping("/permanent")
    public void delete(@PathVariable Integer id) {
        this.service.delete(id);
    }
}