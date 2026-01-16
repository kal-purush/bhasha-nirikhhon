package com.example.chandu1;

import com.example.chandu1.model.Employee;
import com.example.chandu1.service.EmployeeService;
import com.example.chandu1.serviceImpl.EmployeeServiceImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.util.Scanner;
@SpringBootApplication
public class Chandu1Application {

	public static void main(String[] args) {
		EmployeeService service= new EmployeeServiceImpl() {
			@Override
			public void createEmployee() {

			}

			@Override
			public void showEmployeeBasedOnId(int id) {

			}
		};


		SpringApplication.run(Chandu1Application.class, args);
		System.out.println("Welcome employee table");
		Scanner sc=new Scanner(System.in);
		do {
			System.out.println("1. Add Employee\n" +
					"2. Show All Employee\n" +
					"3. Show Employee based on id \n" +
					"4. update the employee\n" +
					"5. Delete the employee\n");
			int ch=sc.nextInt();
			switch (ch){
				case 1:
					Employee emp=new Employee();
					System.out.println("Enter ID :");
					int id=sc.nextInt();
					System.out.println("Enter name :");
					String name=sc.next();
					System.out.println("Enter salary :");
					double salary=sc.nextDouble();
					System.out.println("Enter age :");
					int age=sc.nextInt();
					emp.setId(id);
					emp.setName(name);
					emp.setSalary(salary);
					emp.setAge(age);
					service.createEmployee(emp);
					break;
				case 2:
					service.showAllEmployee();
					break;
				case 3:
					System.out.println("Enter id to show the details ");
					int empid=sc.nextInt();
					service.showEmployeeBasedOnId(empid);
					break;
				case 4:
					System.out.println("Enter id to update the details");
					int empid1=sc.nextInt();
					System.out.println("Enter the new name");
					name=sc.next();
					service.updateEmployee(empid1,name);
					break;
				case 5:
					System.out.println("Enter the id to delete");
					id=sc.nextInt();
					service.deleteEmployee(id);
					break;
				case 6:
					System.out.println("Thank you for using our Application !");
					int status = 0;
					int i = 0;
					System.exit(status);
				default:
					System.out.println("Enter valid choice !");
			}
		}while (true);
	}

}