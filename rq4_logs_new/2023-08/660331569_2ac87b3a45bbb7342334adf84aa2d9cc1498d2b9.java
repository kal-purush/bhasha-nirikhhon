package com.cbfacademy.accounts;

public class Account {
    private double balance;
    private int accountNumber;

    public Account(double balance, int accountNumber){
        this.setBalance(balance);
        this.accountNumber = accountNumber;
    }

    public double getBalance(){
        return balance;
    }

    public void setBalance(double newBalance){
        this.balance = newBalance;
    }

    public void depositFunds( double deposit){
        if(deposit < 0){
            setBalance(balance += 0);
                }else{
            setBalance(balance += deposit);
            } 
        System.out.println("Funds have been deposited to account number :" + this.accountNumber + ". The balance is now " + this.getBalance() +".");
    }

    public void withdrawFunds(double withdraw){

        if(withdraw > balance){
            System.out.println("Insuffiecient funds available to withdraw £" +withdraw+". you can currently withdraw £" + getBalance() +"." );
            System.out.println("£ "+getBalance() +" have been withdrawn from this account.");
            setBalance(balance -= balance); 
            System.out.println("Your new balance is now £" + getBalance() +".");
                    

        // do the math so you only take as much as a 0 balance
            }else{
                setBalance(balance -= withdraw);
                System.out.println("£ "+withdraw +" have been withdrawn from this account. The new balance is now " + getBalance() +".");

            }
    }
    

    }


