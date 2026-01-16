package com.example.demo.taskManeger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class TaskController {
	
	@GetMapping("show")
	public String showview() {
		return "view";
	}
	
}