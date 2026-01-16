package com.sofkau.bank.controllers;

import java.util.UUID;

import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sofkau.bank.commands.DepositCommand;
import com.sofkau.bank.commands.TransferCommand;
import com.sofkau.bank.commands.WithdrawalCommand;
import com.sofkau.bank.entities.Account;
import com.sofkau.bank.entities.Transaction;
import com.sofkau.bank.factories.OperationServiceFactory;
import com.sofkau.bank.http.requests.DepositRequest;
import com.sofkau.bank.http.requests.TransferRequest;
import com.sofkau.bank.http.requests.WithdrawalRequest;
import com.sofkau.bank.http.responses.TransactionResponse;
import com.sofkau.bank.services.accounts.AccountService;
import com.sofkau.bank.services.operations.OperationService;
import com.sofkau.bank.services.transactions.TransactionService;

@RestController
@RequestMapping("/operations")
public class OperationController {
    private final OperationServiceFactory operations;
    private final AccountService accountService;
    private final TransactionService transactionService;

    public OperationController(
            OperationServiceFactory operations,
            AccountService accountService,
            TransactionService transactionService) {
        this.operations = operations;
        this.accountService = accountService;
        this.transactionService = transactionService;
    }

    @PatchMapping("/deposit/account/{number}")
    public TransactionResponse depositFundsToAccount(
            @PathVariable("number") UUID accountNumber,
            @RequestBody DepositRequest depositRequest) {
        Account destination = new Account(); // TODO: should bring back method to query

        DepositCommand depositCommand = DepositCommand.on(destination)
                .with(depositRequest.amount());

        OperationService<DepositCommand> deposit = operations.from(depositCommand);

        deposit.process(depositCommand);

        return TransactionResponse.from(new Transaction()); // TODO: should bring last client transaction
    }

    @PatchMapping("/withdrawal/account/{number}")
    public TransactionResponse withdrawFundsFromAccount(
            @PathVariable("number") UUID accountNumber,
            @RequestBody WithdrawalRequest withdrawalRequest) {
        Account origin = new Account();

        WithdrawalCommand withdrawalCommand = WithdrawalCommand.on(origin)
                .with(withdrawalRequest.amount());

        OperationService<WithdrawalCommand> withdrawal = operations.from(withdrawalCommand);

        withdrawal.process(withdrawalCommand);

        return TransactionResponse.from(new Transaction());
    }

    @PatchMapping("/transfer/account/{number}")
    public TransactionResponse transferFundsFromAccount(
            @PathVariable("number") UUID accountNumber,
            @RequestBody TransferRequest transferRequest) {
        Account origin = new Account();
        Account destination = new Account();

        TransferCommand transferCommand = TransferCommand.on(origin, destination)
                .with(transferRequest.amount());

        OperationService<TransferCommand> transfer = operations.from(transferCommand);

        transfer.process(transferCommand);

        return TransactionResponse.from(new Transaction());
    }
}