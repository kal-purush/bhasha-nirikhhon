package com.sofkau.bank.services.accounts;

import java.util.List;
import java.util.UUID;

import com.sofkau.bank.entities.Account;
import com.sofkau.bank.entities.Client;
import com.sofkau.bank.entities.Account.Type;

public interface AccountService {
    Account createAccount(Account account);

    void updateAccount(UUID number, Account account);

    Type findAccountTypeByName(Type.Name name);

    Account findAccountByNumber(UUID number);

    List<Account> findClientAccounts(Client client);
}