package com.example.GitHubHelloworld;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EmployeeController {
	
	@RequestMapping(value="/data")
	public String getData(){
		
		return "shahimsha Hyderabad";
		
	}

}