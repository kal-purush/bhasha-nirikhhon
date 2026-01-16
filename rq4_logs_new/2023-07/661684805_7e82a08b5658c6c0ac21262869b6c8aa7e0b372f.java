package com.fisco.app.client.impl;

import com.fisco.app.client.TransactionClient;
import com.fisco.app.client.UserClient;
import com.fisco.app.common.CommonClient;
import com.fisco.app.contract.TransactionContract;
import com.fisco.app.entity.Transaction;
import com.fisco.app.mapper.UserMapper;
import com.fisco.app.utils.SpringUtils;
import org.fisco.bcos.sdk.v3.BcosSDK;
import org.fisco.bcos.sdk.v3.model.TransactionReceipt;
import org.fisco.bcos.sdk.v3.transaction.model.exception.ContractException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

@Service
public class TransactionClientImpl extends CommonClient implements ApplicationRunner, TransactionClient {

    @Autowired
    UserMapper userMapper;

    public void record_transaction(int pet_id, String purchase_username, String sell_username, String transaction_date, int price) {
        TransactionContract transactionContract = (TransactionContract) getContractMap().get("TransactionContract");
        transactionContract.record_transaction(BigInteger.valueOf(pet_id), purchase_username, sell_username, transaction_date, BigInteger.valueOf(price));
    }

    public List<Transaction> query_all_transactions() {
        TransactionContract transactionContract = (TransactionContract) getContractMap().get("TransactionContract");
        List<Transaction> transactionList = new ArrayList<>();
        try {
            List<TransactionContract.Transaction> transactions = transactionContract.query_all_transaction();
            for (TransactionContract.Transaction transaction : transactions) {
                transactionList.add(translateTransaction(transaction));
            }
        } catch (ContractException e) {
            System.err.println(getClass().getName() + " : query_all_transactions 抛出异常");
            throw new RuntimeException(e);
        }
        return transactionList;
    }

    public Transaction query_transaction_by_id(int transaction_id) {
        TransactionContract transactionContract = (TransactionContract) getContractMap().get("TransactionContract");
        try {
            TransactionContract.Transaction transaction = transactionContract.query_pid_transaction(BigInteger.valueOf(transaction_id));
            return translateTransaction(transaction);
        } catch (ContractException e) {
            System.err.println(getClass().getName() + " : query_transaction_by_id 抛出异常");
            throw new RuntimeException(e);
        }
    }

    /**
     * 未完成，无法处理的返回值类型
     */
    public List<Transaction> query_transaction_by_owner(String owner) {
        //通过owner查询address
        String address = userMapper.selectByUsername(owner).getAddress();
        TransactionContract transactionContract = (TransactionContract) getContractMap().get("TransactionContract");


        return null;
    }

    private Transaction translateTransaction(TransactionContract.Transaction transaction) {
        return new Transaction(transaction.pid.intValue(), transaction.purchase_username,
                transaction.owner, transaction.transaction_date, transaction.price.intValue());
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        BcosSDK sdk = SpringUtils.getBean("bcosSDK");
        deploy("TransactionContract", TransactionContract.class, sdk);
    }
}