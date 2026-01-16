package com.example.studiograficzne;

public class Employee {
    private String name;
    private double additional_exp1 = 0;
    private double additional_money1 = 0;
    private double salary = 0;


    public double getAdditional_exp1() {
        return additional_exp1;
    }

    public double getAdditional_money1() {
        return additional_money1;
    }

    public double getSalary() {
        return salary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAdditional_exp1(double additional_exp1) {
        this.additional_exp1 = additional_exp1;
    }

    public void setAdditional_money1(double additional_money1) {
        this.additional_money1 = additional_money1;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }
}