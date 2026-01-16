package org.example.coding.moneytransfer;

import org.example.coding.moneytransfer.model.Account;
import org.example.coding.moneytransfer.ops.AccountOperations;

public class MoneyService {

    private final AccountOperations accountOperations;

    public MoneyService(AccountOperations accountOperations) {
        this.accountOperations = accountOperations;
    }

    public void addMoney(int accountId, int amount) {
        Account account = accountOperations.getAccount(accountId);
        account.addToBalance(amount);
    }

    public void extractMoney(int accountId, int amount) {
        Account account = accountOperations.getAccount(accountId);
        account.extractFromBalance(amount);
    }

    public void transferMoney(int fromAccountId, int toAccountId, int amount) {
        Account accountFrom = accountOperations.getAccount(fromAccountId);
        Account accountTo = accountOperations.getAccount(toAccountId);

        accountFrom.extractFromBalance(amount);
        accountTo.addToBalance(amount);
    }

}