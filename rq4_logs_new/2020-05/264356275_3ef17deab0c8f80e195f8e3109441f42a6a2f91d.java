package com.notable.controllers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.notable.business.Cart;

@RestController
public class CartRestController {

	@RequestMapping("cartsave")
	@ResponseBody
	public Cart cartReturn(HttpServletRequest request) throws IOException, ClassNotFoundException {
		
		Cart cart = new Cart();
		FileInputStream fi = new FileInputStream(new File("adminCart.txt"));
		ObjectInputStream oi = new ObjectInputStream(fi);
		cart = (Cart) oi.readObject();
		oi.close();
		fi.close();
		
		
		System.out.println("Inside CartRestController in the Stizzore");
		System.out.println("CART: " + cart.getItems().toString());
		
		
		//Cart cart2 = new Cart();
//		LineItem li = new LineItem();
//		Product p = new Product();
//		li.setLineItemId(1);
//		li.setProduct(p);
//		li.setQuantity(25);
//		
//		cart2.addItem(li);
//		
//		return cart2;
		
		return cart;
	}
}