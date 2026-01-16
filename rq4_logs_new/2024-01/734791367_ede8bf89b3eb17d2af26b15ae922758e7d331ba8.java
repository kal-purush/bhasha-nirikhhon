package com.spring.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.spring.dao.foodOrderDAO;

@Controller
@RequestMapping("/foodOrder")
public class foodOrderController {
	
	@Autowired
	private foodOrderDAO foodOrderDAO;
	
	@GetMapping("/foodOrderList")
	public String foodOrderList() {
		String url="/foodOrder/foodOrderList";
		
		return url;
	}

}