package tec.poo.tareas;

class ImmutableBankAccount {

    private String accountNumber;
    private double balance;

    public ImmutableBankAccount(String accountNumber, double balance) {
        this.accountNumber = accountNumber;
        this.balance = balance;
    }

    public ImmutableBankAccount deposit(double balance, String accountNumber){
        double nbalance;
        nbalance=balance+this.balance;
        ImmutableBankAccount ins = new ImmutableBankAccount(accountNumber, nbalance);
        return ins;
    }


    public ImmutableBankAccount withdraw(double balance, String accountNumber){
        double nbalance;
        nbalance=this.balance-balance;
        ImmutableBankAccount draw = new ImmutableBankAccount(accountNumber, nbalance);
        return draw;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public double getBalance() {
        return balance;
    }
}