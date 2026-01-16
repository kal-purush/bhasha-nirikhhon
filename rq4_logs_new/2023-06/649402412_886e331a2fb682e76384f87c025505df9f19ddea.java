package Mod4;

import java.text.NumberFormat;
import java.util.Scanner;

public class Calc {

    public static void main(String[] args) {

            
      Scanner scanner = new Scanner(System.in);
      
      System.out.println("Please enter beginning principal amount");
      double principal = scanner.nextDouble();
      
      System.out.println("Please enter annual interest rate, e.g. 0.06");
      float annualInterestRate = scanner. nextFloat();
      
      System.out.println("Please enter number of payment periods (e.g. total # of months)");
      int paymentPeriods = scanner.nextInt();
      
      float interestRate = annualInterestRate / 12;
      
      double monthlyPayments = principal * (
            (interestRate * (Math.pow(1 + interestRate, paymentPeriods))) /
           ((Math.pow(1 + interestRate, paymentPeriods))-1)
              );
     
      
      System.out.println("Monthly payment: " + NumberFormat.getCurrencyInstance().format(monthlyPayments));
      
      System.out.printf("%15s %15s %15s %15s %15s%n",
              "Current Month ","Starting Principal", "Interest Payment", 
              "Principal Payment", "Ending Principal");
      
      
      
      
    }  
      
}
          
      