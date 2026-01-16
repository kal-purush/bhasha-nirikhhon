package com.rh.Internship.employee;

/**
 * @author Saeed Zarinfam
 */
public class SalariedEmployee extends Employee {
\
    private long monthlySalary;

    public SalariedEmployee(String name, String code, long monthlySalary) {
        super(name, code);
        this.monthlySalary = monthlySalary;
    }

    @Override
    public float calculateSalary() {
        return monthlySalary;
    }

    public long getMonthlySalary() {
        return monthlySalary;
    }

    public void setMonthlySalary(long monthlySalary) {
        this.monthlySalary = monthlySalary;
    }
}