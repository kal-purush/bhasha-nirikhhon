package com.sergtm.application.copyAndPaste;

/**
 * Created by admin on 29.07.2016.
 */
public class Main {
    public static void main(String[] args){
        SavingsBankAccount savingsBankAccount = new SavingsBankAccount();
        RegularBankAccount regularBankAccount = new RegularBankAccount();
            regularBankAccount.setNumber("123");
            regularBankAccount.setBalance(100);
            regularBankAccount.setAmountOfCostlyOperations(7);

            savingsBankAccount.setNumber("888");
            savingsBankAccount.setBalance(800);
            savingsBankAccount.setInterestRate(0.03);



    }
    public ResultBA displayRegularBA(RegularBankAccount regularBankAccount){
        ResultBA resultBA = new ResultBA();
            resultBA.setResultBalance(regularBankAccount.getBalance());
            resultBA.setResultNumber(regularBankAccount.getNumber());
        return resultBA;
    }
    public ResultSBA  displaySavingsBA(SavingsBankAccount savingsBankAccount){

        ResultSBA resultSBA = new ResultSBA();
        resultSBA.setResultBalance(savingsBankAccount.getBalance());
        resultSBA.setResultNumber(savingsBankAccount.getNumber());
        return resultSBA;
    }
}