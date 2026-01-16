package com.knightriders.transactiontool.controllers;


import com.knightriders.transactiontool.dto.TransactionDTO;
import com.knightriders.transactiontool.entities.Transaction;
import com.knightriders.transactiontool.services.TransactionService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

    private final TransactionService transactionService;

    public TransactionController(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @PostMapping
    public Transaction performTransaction(@RequestBody TransactionDTO transactiondto) {
        return transactionService.performTransaction(transactiondto);
    }

    @GetMapping
    public List<Transaction> getTransactions() {
        return transactionService.getTransactions();
    }
}