package simple

import java.math.BigDecimal;

-banking-system;

public class Account {
    private Integer accountNumber;
    private BigDecimal balance;
    private String name; 

    public Account(Integer accountNumber, BigDecimal balance, String name){
        this.accountNumber = accountNumber;
        this.balance = balance;
        this.name = name; 
    }

    public Account () {
        this(0, 0.0, "template"); 
    }

    public Integer getAccountNumber () {
        return this.accountNumber;
    }

    public BigDecimal getBalance() {
        return this.balance;
    }

    public String getName () {
        return this.name; 
    }

    public void setAccountNumber(int accountNumber) {
        this.accountNumber = accountNumber; 
    }

    public void setBalance (BigDecimal balance) {
        this.balance = balance; 
    }

    public void setName(String name) {
        this.name = name; 
    }

    public BigDecimal deposit (BigDecimal deposit) {
        BigDecimal newBalance; 
        newBalance = this.balance.add(deposit); 

        return newBalance;
    }

    public BigDecimal withdraw (BigDecimal withdraw) {
        BigDecimal newBalance; 

        newBalance = this.balance.subtract(withdraw); 
    }



    


    
}