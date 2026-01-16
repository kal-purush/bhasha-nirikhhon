package com;

public class Konto {
    private int accountNumber;
    private double accountBalance;
    private double debt;

    public Konto(int accountNumber, double accountBalance, double debt) {
        this.accountNumber = accountNumber;
        this.accountBalance = accountBalance;
        this.debt = debt;
    }

    public int getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(){
        this.accountNumber = accountNumber;
    }

    public double getAccountBalance() {
        return accountBalance;
    }

    public void setAccountBalance(){
        this.accountBalance = accountBalance;
    }

    public double getDebt() {
        return debt;
    }

    public void setDebt(){
        this.debt = debt;
    }


}




