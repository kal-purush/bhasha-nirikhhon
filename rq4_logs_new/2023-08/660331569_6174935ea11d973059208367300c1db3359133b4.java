package com.cbfacademy.accounts;

public class Bank {
        public static void main(String[] args) {
 
        Account account = new Account(1500, 875093);

        System.out.println("Your  account balance is £" + account.getBalance());  
        account.depositFunds(120);
        account.withdrawFunds(2000);

        CurrentAccount currentAccount = new CurrentAccount(200, 875093, 1000);
        System.out.println("Your current account balance is £" +currentAccount.getBalance());
        currentAccount.setOverdraftLimit(2000.00);
        currentAccount.withdrawFunds(-10);
        
        
        SavingsAccount savingsAccount = new SavingsAccount(1500, 875093,1.25);
        System.out.println(account.getBalance());  
        System.out.println("Your savings account balance is £"+ savingsAccount.getBalance()+".");    
        savingsAccount.depositFunds(1250);
        savingsAccount.addInterest();      
    }
}