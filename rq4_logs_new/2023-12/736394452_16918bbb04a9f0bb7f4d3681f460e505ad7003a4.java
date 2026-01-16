import java.util.*;

import accounts.Account;

public class Costumer {
    private String name; 
    private String address;
    private String phoneNumber; 
    private List<Account> accounts; 

    public Costumer (String name, String address, String phoneNumber) {
        this.name = name;
        this.address = address; 
        this.phoneNumber = phoneNumber;
        this.accounts = new ArrayList<>(); 
    }

    public Costumer() {
        this("John Smith", "0 Street, Kelowna, British Columbia, Canada", "000-000-0000"); 
    }

    public String getName () {
        return this.name;
    }

    public String getAddress () {
        return this.address;
    }

    public String getPhoneNumber () {
        return this.phoneNumber;
    }

    public void setName (String name) {
        this.name = name;
    }

    public void setAddress (String address) {
        this.address = address;
    }

    public void setPhoneNumber (String phoneNumber) {
        this.phoneNumber = phoneNumber; 
    }

    public void addAccount(Account account) {
        accounts.add(account); 
    }

    public void printCostumerInfo() {
        System.out.println("CostumerID: " + this.costumerId);
        System.out.println("Costumer Name: " + name);
        System.out.println("Accounts: ");

        for(Account account : accounts) {
            System.out.println("Account Number: " + account.getAccountNumber());
            System.out.println("Balance: $" + account.getBalance());
            System.out.println("-------------------------------------------------");
        }

    }



    
}