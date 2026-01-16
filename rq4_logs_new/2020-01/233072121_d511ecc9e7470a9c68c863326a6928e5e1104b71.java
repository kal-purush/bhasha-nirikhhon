package com;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Account {

    private HashMap<String, String> details = new HashMap<>();

    public Account(int accountNumber, double accountBalance, double debt, int ssn) {
        this.details.put("accountnumber", String.valueOf(accountNumber));
        this.details.put("accountbalance", String.valueOf(accountBalance));
        this.details.put("debt", String.valueOf(debt));
        this.details.put("ssn", String.valueOf(ssn));
    }

    public void setAccountNumber(int accountNumber){
        this.details.put("accountnumber", String.valueOf(accountNumber));
    }

    public int getAccountNumber() {
        return Integer.parseInt(details.get("accountnumber"));
    }

    public void setAccountBalance(double accountBalance){
        this.details.put("accountbalance", String.valueOf(accountBalance));
    }

    public double getAccountBalance() {
        return Double.parseDouble(details.get("accountbalance"));
    }

    public void setDebt(double debt){
        this.details.put("debt", String.valueOf(debt));
    }

    public double getDebt() {
        return Double.parseDouble(details.get("debt"));
    }

    public void setSsn(int ssn) {
        details.put("ssn", String.valueOf(ssn));
    }

    public int getSsn() {
        return Integer.parseInt(details.get("ssn"));
    }

    public List<String> getList() {
        List<String> detailsList = new ArrayList<>();
        for(String key:details.keySet()) {
            detailsList.add(key+":"+details.get(key));
        }
        //System.out.println(detailsList);
        return detailsList;
    }
}