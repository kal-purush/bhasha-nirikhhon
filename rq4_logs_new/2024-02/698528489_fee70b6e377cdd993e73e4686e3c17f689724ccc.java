package com.sample1;

import java.util.Scanner;

class Employee {
	String name;
	String dateOfJoining;
	String gender;
	double salary;
}

public class SalaryCalculator {

	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		Employee[] employees = new Employee[7];
		for (int i = 0; i < employees.length; i++) {
			employees[i] = new Employee();
			
			System.out.println("Enter details for Employee " + (i + 1) + ":");
			System.out.print("Name: ");
			employees[i].name = sc.nextLine();
			System.out.print("Date of Joining: ");
			employees[i].dateOfJoining = sc.nextLine();
			System.out.print("Gender (Male/Female): ");
			employees[i].gender = sc.nextLine();
			System.out.print("Salary: ");
			employees[i].salary = sc.nextDouble();
			sc.nextLine();
		}
		double highestMaleSalary = 0;
		double highestFemaleSalary = 0;
		double totalMaleSalary = 0;
		double totalFemaleSalary = 0;
		int maleCount = 0;
		int femaleCount = 0;

		for (Employee emp : employees) {
			if (emp.gender.equalsIgnoreCase("Male")) {
				totalMaleSalary += emp.salary;
				if (emp.salary > highestMaleSalary) {
					highestMaleSalary = emp.salary;
				}
				maleCount++;
			} else if (emp.gender.equalsIgnoreCase("Female")) {
				totalFemaleSalary += emp.salary;
				if (emp.salary > highestFemaleSalary) {
					highestFemaleSalary = emp.salary;
				}
				femaleCount++;
			}
		}
		
		double avgMaleSalary = totalMaleSalary / maleCount;
		double avgFemaleSalary = totalFemaleSalary / femaleCount;

		System.out.println("\nResults:");
		System.out.println("Highest Salary for Male Employees: " + totalMaleSalary);
		System.out.println("Average Salary for Male Employees: " + avgMaleSalary);

		System.out.println("\nHighest Salary for Female Employees: " + totalFemaleSalary);
		System.out.println("Average Salary for Female Employees: " + avgFemaleSalary);

		sc.close();
	}

}