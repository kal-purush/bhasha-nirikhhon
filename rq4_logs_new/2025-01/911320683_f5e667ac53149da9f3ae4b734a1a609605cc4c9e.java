package com.sofkau.bank.services.transactions;

import java.util.List;

import org.springframework.stereotype.Service;

import com.sofkau.bank.entities.Account;
import com.sofkau.bank.entities.Transaction;
import com.sofkau.bank.entities.Transaction.Action;
import com.sofkau.bank.exceptions.AlreadyExistsException;
import com.sofkau.bank.exceptions.NotFoundException;
import com.sofkau.bank.repositories.transactions.TransactionActionRepository;
import com.sofkau.bank.repositories.transactions.TransactionRepository;

@Service
public class TransactionServiceImpl implements TransactionService {
    private final TransactionRepository transactionRepository;
    private final TransactionActionRepository transactionActionRepository;

    public TransactionServiceImpl(
            TransactionRepository transactionRepository,
            TransactionActionRepository transactionActionRepository) {
        this.transactionRepository = transactionRepository;
        this.transactionActionRepository = transactionActionRepository;
    }

    @Override
    public Transaction createTransaction(Transaction transaction) {
        if (transaction.getAccount() == null)
            throw new NotFoundException();

        if (transaction.getId() > 0)
            throw new AlreadyExistsException();

        return transactionRepository.save(transaction);
    }

    @Override
    public Action findTransactionActionByName(Action.Name name) {
        String actionName = name.name();

        return transactionActionRepository.findByName(actionName)
                .orElseThrow(NotFoundException::new);
    }

    @Override
    public List<Transaction> findAccountTransactions(Account account) {
        return transactionRepository.findAllByAccount(account);
    }
}