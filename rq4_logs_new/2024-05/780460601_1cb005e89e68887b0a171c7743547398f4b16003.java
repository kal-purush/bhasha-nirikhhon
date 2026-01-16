package com.hishamfactory;

import java.util.Scanner;

public abstract class FullTimeEmployee extends Employee{
    protected double bonuses;
    protected double insurance_premium;
    Scanner sc = new Scanner(System.in);
    public FullTimeEmployee(String first_name,String last_name,int age,String tel_number,String address,String role,double basic_salary,String employee_pin,Company company){
        super(first_name,last_name,age,tel_number,address,role,basic_salary,employee_pin,company);
        this.tax = (double) 15 / 100;
    }

    @Override
    public double calculateSalary() {
            FullTimeEmployee fullTimeEmployee = this;
            System.out.print("Enter employee bonuses: ");
            fullTimeEmployee.bonuses = sc.nextDouble();
            sc.nextLine();
            System.out.print("Enter insurance_premium: ");
            fullTimeEmployee.bonuses = sc.nextDouble();
            sc.nextLine();
            double total_tax = fullTimeEmployee.getBasic_salary() * this.getTax();
            return fullTimeEmployee.net_salary = fullTimeEmployee.basic_salary + fullTimeEmployee.bonuses + fullTimeEmployee.insurance_premium - total_tax;
    }
}