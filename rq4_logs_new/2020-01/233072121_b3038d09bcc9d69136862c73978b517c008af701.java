package com;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;



public class Transaction {
    public int accountNumber = 1111111111;
    public double transactionsSum = 200.00;
    public double balance = 700;
    public Date date = new Date(2020,10, 01);


    public int getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(int accountNumber) {
        this.accountNumber = accountNumber;
    }

    public double getTransactionsSum() {
        return  transactionsSum;
    }

    public void setTransactionsSum(double transactionsSum) {
        this.transactionsSum = transactionsSum;
    }

    public double getBalance(double balance) {
        this.balance = balance;
        return balance;
    }

    /*public double setBalance(double balance) {
        this.balance = balance;
        return balance;
    }*/

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public List <Integer> transaction = new ArrayList<Integer>();



    public void getTransaction() {
        double t = getBalance((int)23.00);

        transaction.add(1234500);
        transaction.add(1234500);
    }

    public void getListTransaction(){
        for( Object t : transaction){
            System.out.println(t);
        }
    }






    /*private LinkedHashMap<String, String> transaction = new LinkedHashMap<>();


    public Transaction(int accountNumber, double transactionNumber, double balance) {
        transaction.put("accountNumber", String.valueOf(accountNumber));
        transaction.put("transactionnumber", String.valueOf(transactionNumber));
        transaction.put("balance", String.valueOf(balance));
    }

    public int getAccountNumber() {

        return Integer.parseInt(transaction.get("accountNumber"));
    }

    public void setAccountNumber(int accountNumber) {
        this.transaction.put("accountNumber", String.valueOf(accountNumber));
    }


    public double getTransactionNumber() {

        return Integer.parseInt(transaction.get("transactionNumber"));
    }

    public void setTransactionNumber(double transactionNumber) {
        this.transaction.put("transactionNumber", String.valueOf(transactionNumber));
    }

    public double getBalance() {
        return Integer.parseInt(transaction.get("balance"));

    }

    public void setBalance(double balance) {
        this.transaction.put("transactionNumber", String.valueOf(balance));
    }

    public List<String> getList() {
        List<String> transactionList = new ArrayList<>();
        for(String key : transaction.keySet()) {
            transactionList.add(key+":"+transaction.get(key));
        }
        return transactionList;
    }*/
}