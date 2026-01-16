package com.example.mjhbs1.controllers;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.example.mjhbs1.util.Util;

@Controller
public class UserController  {
	@PostMapping("/saveData")

	public String saveData(
			@RequestParam("dep") String dep,
			@RequestParam("name") String name,
			@RequestParam("lastName") String lastName,
			@RequestParam("idNumber") String idNumber,
			@RequestParam("birthdate") String birthdate, ModelMap modelMap) {
		if (Util.isDateValid(birthdate)) {
			System.out.println("valid Birthdate");
		}
		modelMap.put("dep", dep);
		modelMap.put("name", name);
		modelMap.put("lastName", lastName);
		modelMap.put("idNumber", idNumber);
		modelMap.put("birthdate", birthdate);
		return "validator.jsp";
	}
	 	
	}