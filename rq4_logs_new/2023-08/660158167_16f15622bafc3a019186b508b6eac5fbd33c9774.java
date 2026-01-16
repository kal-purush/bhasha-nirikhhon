package com.dragos.businessLogic;

import java.util.UUID;

public class Account {

    private double funds;
    private int accountId;

    public Account(double funds){
        setFunds(funds);
    }

    public Account(int accountId,double funds){
        this.accountId = accountId;
        setFunds(funds);
    }
    public void setFunds(double funds) {
        this.funds = funds;
    }
    public double getFunds() {
        return funds;
    }
    public int getAccountId() {
        return accountId;
    }





    public void transferFunds(Account receiver, double amount){
    }


}