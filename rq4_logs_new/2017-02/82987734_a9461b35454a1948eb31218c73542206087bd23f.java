package com.liuhaozzu.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import com.liuhaozzu.service.ProductService;

@Controller("productController")
public class ProductController {
	
	@Autowired
	private ProductService productService;

	@RequestMapping("/product")
	public String getProduct(Model model){
		
		Map<String, String> obj = productService.getProduct("123");
		model.addAttribute("message","hello world");
		return "product";
	}
}