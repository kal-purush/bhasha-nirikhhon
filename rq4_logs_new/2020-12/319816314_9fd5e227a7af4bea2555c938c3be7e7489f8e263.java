package com.java.excercises.operations;

public class SalaryManagement {

	public double getRisedSalary(double salary) {
		double newSalary = 0.0;

		if (salary >= 50000) {
			newSalary = ((salary) + (salary * 0.03));

		} else if (salary >= 30000 && salary < 50000) {
			newSalary = ((salary) + (salary * 0.05));
		}

		else if (salary >= 10000 && salary < 30000) {
			newSalary = ((salary) + (salary * 0.10));
		}

		else if (salary <= 10000) {
			newSalary = ((salary) + (salary * 0.15));
		}
		return newSalary;
	}

	public static void main(String[] args) {
		SalaryManagement objectSalary = new SalaryManagement();
		double resultado;
		resultado = objectSalary.getRisedSalary(5000);
		System.out.println("El nuevo salario es de: " + resultado);

	}
}