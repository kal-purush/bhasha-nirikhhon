package com.fibonacci.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.fibonacci.service.IGreetingService;

@RestController
public class GreetingController {

	@Autowired
	private IGreetingService greetingService;

	@GetMapping(path = "/greet/{name}")
	public String greet(@PathVariable  String name) {

		return greetingService.greet(name);
	}
	
	
	
 
	
}