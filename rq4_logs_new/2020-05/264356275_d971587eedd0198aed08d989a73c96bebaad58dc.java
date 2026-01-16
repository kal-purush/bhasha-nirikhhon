package com.notable.controllers;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;

import com.notable.business.Product;
import com.notable.data.ProductMapper;

@Controller
public class SearchController {

	@Autowired
	JdbcTemplate jdbc;

	@PostMapping("search")
	public String registerLoginUser(String searchTerm, HttpServletRequest request) {

		System.out.println(searchTerm);

		List<Product> products = jdbc.query("SELECT * FROM products where searchTerms LIKE '%" + searchTerm + "%'",
				new ProductMapper());

		HttpSession session = request.getSession();
		session.setAttribute("products", products);

		return "views/search";

	}
}