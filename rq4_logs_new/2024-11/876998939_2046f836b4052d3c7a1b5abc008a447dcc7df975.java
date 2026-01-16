package com.smartverse.churchlitebackend.services.transactions;

import com.smartverse.churchlitebackend_gen.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TransactionsService {

    @Autowired
    private TransactionsRepository transactionsRepository;

    public void save(FinancialEntity entity) {

        if(entity.getPaymentReceiptDate() != null){

            var transaction = new TransactionsEntity();
            transaction.setPerson(entity.getPerson());
            transaction.setFinancial(entity);
            transaction.setDescription("LANÇAMENTO DE DUPLICATA - RECEITA");
            transaction.setDateTransaction(entity.getIssueDate());
            transaction.setValue(entity.getValue());

            if(entity.getTypeFinancial() == TypeFinancial.REVENUE){
                transaction.setTransactionOperation(TransactionOperation.REVENUE);
            } else{
                transaction.setTransactionOperation(TransactionOperation.EXPENSE);
            }
            transactionsRepository.save(transaction);
        }

    }
}