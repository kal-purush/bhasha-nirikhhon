package com.sinensia.polloshermanos.backend.presentation.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/polloshermanos")
public class AppHomeController {

	@GetMapping("/home")
	public String goHome() {
		return "home";
	}

}