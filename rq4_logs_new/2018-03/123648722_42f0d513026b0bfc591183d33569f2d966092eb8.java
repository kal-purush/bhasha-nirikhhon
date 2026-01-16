package model;

/**
 * Created by Asif on 3/14/2018.
 */

public class Account {

    int userid;
    String firstname,lastname,emailaddress,accounttype;
    boolean accountstatus;

    public Account() {
    }

    public int getUserID() {
        return userid;
    }

    public void setUserID(int userid) {
        this.userid = userid;
    }

    public String getFirstName() {
        return firstname;
    }

    public void setFirstName(String firstname) {
        this.firstname = firstname;
    }

    public String getLastName() {
        return lastname;
    }

    public void setLastName(String lastname) {
        this.lastname = lastname;
    }

    public String getEmailAddress() {
        return emailaddress;
    }

    public void setEmailAddress(String emailaddress) {
        this.emailaddress = emailaddress;
    }

    public boolean getAccountStatus() {
        return accountstatus;
    }

    public void setAccountStatus(boolean accountstatus) {
        this.accountstatus = accountstatus;
    }

    public String getAccountType() {
        return accounttype;
    }

    public void setAccountType(String accounttype) {
        this.accounttype = accounttype;
    }
}