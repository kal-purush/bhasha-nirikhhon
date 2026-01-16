package com.notable.controllers;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import com.notable.business.Product;
import com.notable.data.CategoryMapper;

@Controller
public class CategoryController {
	
	@Autowired
	JdbcTemplate jdbc;
	
	@GetMapping("category")
	public String displayProductsByCategory(String name) {
		
		String sqlQuery;
		
		List<Product> products = jdbc.query("SELECT * FROM products where category = " + name, new CategoryMapper());
		
		for (Product product : products) {
			
		}
		
		
		return "";
	}

}