package com.docsmiles;

/**
 * Created by Doctor Smiles on 12/17/2016.
 */


public class CustomerAccount {
    private int acctNum;
    private double acctBalance;
    private String custName;
    private String acctEmail;
    private String acctPhone;
    // private String fuckboi; // testing IntelliJ's "generate" function to automatically
                            // create Getters & Setters for us. Yes, I am very mature
                            // when it comes to naming things.
// Constructors for our actual lesson:
    public CustomerAccount() {
        this(12345, 2.50, "John Doe", "guymcperson@place.com", "123-456-7890");
        // Note: This is a special case usage of the "this" keyword. When building
        // constructors and setting default values for parameters, "this" must be the
        // first line of the constructor.
        System.out.println("Empty constructor called!");}
    // This is really the same thing as the above class, hence when you use this,
    // it'll do everything the class with the same name can, but then it
    // also automatically prints this string at the beginning too,
    // regardless of what else happens, because you didn't include any
    // parameters like we have in the constructor we made below.
    // We also include some default values for our variables by calling the constructor
    // below and using its parameters.

    public CustomerAccount(int acctNum, double acctBalance, String custName, String acctEmail,
                           String acctPhone) {
        // System.out.println("Account constructor with parameters called.");
        this.acctNum = acctNum;
        this.acctBalance = acctBalance;
        this.custName = custName;
        this.acctEmail = acctEmail;
        this.acctPhone = acctPhone;
    }




    // Getters & Setters

/*
// IntelliJ made these for us, how kind.
    public String getFuckboi() {
        return fuckboi;
    }

    public void setFuckboi(String fuckboi) {
        this.fuckboi = fuckboi;
    }
*/
    public void setAcctNum(int acctNum) {
        this.acctNum = acctNum;
    }

    public int getAcctNum() {
        return this.acctNum;
    }

    public void setAcctBalance(double acctBalance) {
        if (acctBalance >= 0) {
            this.acctBalance = acctBalance;
        } else {
            System.out.println("You have no money! D:");
        }
    }

    public double getAcctBalance() {
        return acctBalance;
    }

    public void setCustName(String custName) {
        this.custName = custName;
    }

    public String getCustName() {
        return this.custName;
    }

    public void setAcctEmail(String acctEmail) {
        this.acctEmail = acctEmail;
    }

    String getAcctEmail() {
        return this.acctEmail;
    }

    void setAcctPhone(String acctPhone) {
        this.acctPhone = acctPhone;
    }

    public String getAcctPhone() {
        return this.acctPhone;
    }

    /*
    // Attempted to make a one-in-all set method for Main class usage, so instead of having
    // five or however many separate lines, we could boil it down to just one or two.
    // Same for get. Couldn't figure out how, though. Figured I'd save it for later
    public void fuck -> {
        return
    }
    public void setCredentials(person.setAcct)
*/

    // Deposit/Withdraw
    public double depositFunds(double depAmt) {
        if (depAmt >= 0) {
            this.acctBalance += depAmt;
            System.out.println(custName + " has deposited: $" + depAmt);

            System.out.println("New total balance is: $" + String.format("%.2f", acctBalance));
            return depAmt;
        } else {
            withdrawFunds(-depAmt);
            return -1;
        }
    }

    public double withdrawFunds(double withdrawAmt) {
        if (withdrawAmt > 0) {
            if (withdrawAmt < acctBalance) {
                this.acctBalance -= withdrawAmt;
                System.out.println(custName + " has withdrawn: $" + withdrawAmt);
                System.out.println("New total balance is: $" + String.format("%.2f", acctBalance));


            } else {
                System.out.println("Attempted withdrawal of $" + withdrawAmt );
                System.out.println("You don't have that much money. Withdrawal not processed.");
            }

        }
        return withdrawAmt;
    }

// End of Class
}