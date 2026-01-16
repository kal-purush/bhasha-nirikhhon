package com.javalearning.springboot.helloworld.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.javalearning.springboot.helloworld.model.Employee;
import com.javalearning.springboot.helloworld.service.EmployeeService;

@RestController
public class EmployeeController {
	
	@Autowired
	EmployeeService employeeService;

	@GetMapping("/employee")
	public List<Employee> getAll(){
		return employeeService.getAll();
	}
}