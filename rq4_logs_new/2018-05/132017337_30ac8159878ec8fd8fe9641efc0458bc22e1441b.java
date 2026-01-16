package com.collectorsAndThen;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.collectorsSummarizing.Employee;

public class CollectorsAndThen {

	public static void main(String[] args) {
		
		 List<Employee> employeeList
	      = Arrays.asList(new Employee("Tom Jones", 45, 15000.00,190),
	      new Employee("Tom Jones", 45, 7000.00,220),
	      new Employee("Ethan Hardy", 65, 8000.00,1008),
	      new Employee("Nancy Smith", 22, 10000.00,5),
	      new Employee("Deborah Sprightly", 29, 9000.00,45));
		 
		String maxSalaryEmp = employeeList.stream()
				.collect(Collectors.collectingAndThen(Collectors.maxBy(Comparator.comparing(Employee::getSalary)),
			    (Optional<Employee> emp) -> emp.isPresent() ? emp.get().getName() : "none"));
		System.out.println("Max salaried employee's name: " + maxSalaryEmp);
		 
	}

}