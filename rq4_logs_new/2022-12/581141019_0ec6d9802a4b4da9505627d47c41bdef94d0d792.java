package com.ems.ThymeLeaf.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.ems.ThymeLeaf.entity.Employee;

@Controller
public class WebController {
	
	@RequestMapping("/home")
	public String index() {
		return "home";
	}
	@RequestMapping("/showall")
	public String showall() {
		return "showemp";
	}
	@RequestMapping("/addemp")
	public String add_emp() {
		return "add_emp";
	}
	
	@PostMapping("/register")
	public String empRegister(@ModelAttribute Employee e) {
		
		System.out.println(e);
		return "add_emp";
	}
}