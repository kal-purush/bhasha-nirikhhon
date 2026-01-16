package com.upskill.java_1;

public class Methodtype { 
	
	
	
	/* 	Types of Methods
	 
		1.Void Method
		2.Static Method
		3.Return Type Method  
	 */

	public static int hourlyIncome = 55;

	public static void main(String[] args) {
		Methodtype obj = new Methodtype();
		obj.annualIncomeVoid();
		
		System.out.println("My Monthly Income = " + obj.monthlyIncomeReturn());
		
		weeklyIncomeStatic();
	}

	//void MethodType
	public void annualIncomeVoid(){
		int calculateAnnualIncome = hourlyIncome * 2000;
		System.out.println("My Annual Income = " + calculateAnnualIncome);
	}
	
	//static Method 
	public static void weeklyIncomeStatic(){
		int calculateWeeklyIncome = hourlyIncome * 40;
		System.out.println("My Weekly Income = " + calculateWeeklyIncome);
	}
	
	//return type Method
	public int monthlyIncomeReturn(){
		int calculateMonthlyIncome = hourlyIncome * 180;
		return calculateMonthlyIncome;
	}

}