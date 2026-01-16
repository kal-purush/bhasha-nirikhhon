package com.cvds.lab7;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.cvds.lab7.model.Employee;
import com.cvds.lab7.service.EmployeeService;

@SpringBootApplication
public class SpringBootJpaMediumDemoApplication {

	@Autowired(required = true)
	EmployeeService employeeService;
	public static void main(String[] args) {
		SpringApplication.run(SpringBootJpaMediumDemoApplication.class, args);
	}

	@Bean
	public CommandLineRunner run() throws Exception {
		return (args) -> {
			
			System.out.println("Adding employees....\n");
			employeeService.addEmployee(new Employee("Hank", "Hill", "Salesman", 63000.00));
			employeeService.addEmployee(new Employee("Peggy", "Hill", "Substitute", 43000.00));
			employeeService.addEmployee(new Employee("Homer", "Simpson", "Plant Operator", 163000.00));
			employeeService.addEmployee(new Employee("Rick", "Sanchez", "Scientis", 633000.00));
			employeeService.addEmployee(new Employee("Bob", "Belcher", "Fry Cook", 10.00));
			
			System.out.println("\nGetting all employees....");
			employeeService.getAllEmployees().forEach(employee -> System.out.println(employee));
			
			System.out.println("\nGetting employee with id = 1....");
			System.out.println(employeeService.getEmployee(1L));
			
			System.out.println("\nUpdating employee with id = 1....");
			System.out.println("Before----->" + employeeService.getEmployee(1L));
			Employee employee = employeeService.getEmployee(1L);
			employee.setFirstName("Marge");
			System.out.println("After------>" + employeeService.updateEmployee(employee));
			
			System.out.println("\nDeleting employee....");
			employeeService.deleteEmployee(1L);
		};
	}

}