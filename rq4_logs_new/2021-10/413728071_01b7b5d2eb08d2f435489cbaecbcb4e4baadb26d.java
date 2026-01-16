package com.moglix.employeemanagmentsystem;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import com.moglix.employeemanagementsystem.dto.Employee;
import com.moglix.employeemanagementsystem.service.EmployeeService;
import com.moglix.employeemanagmentsystem.config.Config;

public class Main6 {
	
	@Autowired
	EmployeeService employeeService;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		//You have to create main5 object
		
		
		
		AnnotationConfigApplicationContext applicationContext= new AnnotationConfigApplicationContext(Config.class);
		//get bean
		
		/*applicationContext.scan("com.moglix.employeemanagementsystem.service");
		applicationContext.scan("com.moglix.employeemanagementsystem.repository");
		applicationContext.refresh();
		*/
		
		EmployeeService employeeServiceI=applicationContext.getBean(EmployeeService.class);
		String result=employeeServiceI.addEmpployee(new Employee("123", "VISHU", "Saini", 123.0));
		System.out.println(result);
		
		EmployeeService employeeService2=applicationContext.getBean(EmployeeService.class);
		
		System.out.println(employeeServiceI==employeeService2);
		System.out.println(employeeServiceI.hashCode());
		System.out.println(employeeService2.hashCode());
		System.out.println(employeeServiceI.equals(employeeService2));
		
		

	}

}