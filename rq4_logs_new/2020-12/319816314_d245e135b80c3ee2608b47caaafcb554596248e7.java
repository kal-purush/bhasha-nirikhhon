package com.java.excercises.operations;

public class Loan {

	public double calculateLoan(double salary) {
		double loan = 0.0;

		if (salary >= 80000) {
			loan = (5000000);	
		}
		else if (salary >= 50000 && salary < 80000) {
			loan = (3000000);
		}
		else if (salary >= 20000 && salary < 50000) {
			loan = (500000);
		}
		else if (salary <= 20000) {
			loan = (500000);
		}
		return loan;
	}
	public double calculateInterest(double loan) {
		double bankinterest = 0.0;
		
			bankinterest = ((loan) + (loan * .05));
			
			return bankinterest;
}
	
	public static void main(String[] args) {
		Loan objectLoan = new Loan();
		Loan objectInterest = new Loan();
		double resultado;
		double Interest;
		resultado = objectLoan.calculateLoan(50000);
		Interest = objectInterest.calculateInterest(.05);
		System.out.println("El prestamo es de: " + resultado + " con un interes de: " + Interest);
}
}