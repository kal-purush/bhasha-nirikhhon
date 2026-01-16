package Entities;

import Entities.Expenses;

import java.util.List;
public class User {
    private String fistName;
    private String lastName;
    private String userName;
    private double income;
    private List <Expenses> expensesList;
    public User (String fistName, String lastName,String userName, double income){
        this.fistName = fistName;
        this.lastName= lastName;
        this.userName = userName;
        this.income=income;
    }
    public String getFistName() {
        return fistName;
    }
    public String getLastName() {
        return lastName;
    }
public String getUserName (){
        return userName;
}
    public double getIncome() {
        return income;
    }
    public void setExpensesList (List<Expenses> expensesList){
        this.expensesList =expensesList;
    }
    public List<Expenses> getExpensesList (){
        return expensesList;
    }
    public String toString (){
        return getFistName()+ " " + getLastName() + "\n" +
                "Entities.User name: " + getUserName() + "\n Earns: " + getIncome();
    }
}