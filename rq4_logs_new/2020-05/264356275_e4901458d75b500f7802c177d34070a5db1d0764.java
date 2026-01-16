package com.notable.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import com.notable.business.Product;
import com.notable.data.ProductJDBCTemplate;

@Controller
public class ProductController {

	@Autowired
	ProductJDBCTemplate jdbc;
	
	
	@GetMapping("product")
	public String displayProduct(int productId, HttpServletRequest request, HttpServletResponse response) {
		Product prod = jdbc.getProduct(productId);
		HttpSession session = request.getSession();
		session.setAttribute("product", prod);
		return "views/product";
	}
}