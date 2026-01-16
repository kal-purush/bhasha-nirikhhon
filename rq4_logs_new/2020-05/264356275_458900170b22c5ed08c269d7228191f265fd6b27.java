package com.notable.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;

import com.notable.business.Cart;
import com.notable.business.LineItem;
import com.notable.business.Product;
import com.notable.business.User;
import com.notable.data.ProductJDBCTemplate;

@Controller
public class SuppliesController {

	
	@Autowired
	ProductJDBCTemplate jdbc;
	
	@PostMapping("addSupplies")
	public String orderSupplies(HttpServletRequest request, Integer id, Integer quantity) {

		System.out.println("In the supplies Contoller");
		System.out.println(id);
		
		HttpSession session = request.getSession();
		User user = (User) session.getAttribute("user");
		Cart cart = (Cart) session.getAttribute("adminCart");
		Product prod = jdbc.getProduct(id);
	
		LineItem li = new LineItem();
		li.setProduct(prod);
		li.setQuantity(quantity);
		
		if (cart == null /* && user != null */) {
			// Create line item
			// add to cart
			cart = new Cart();
		}
		
		cart.addItem(li);
		session.setAttribute("adminCart", cart);
		
		System.out.println("Count: " + cart.getCount());
		
		return "views/admin";
	}
	
	@PostMapping("adminSubmitOrder")
	public String submitOrder(HttpServletRequest request) {
		
		
		
		return "";
	}
	
	
}