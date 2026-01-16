/*
 * Controller to perform Cart operations
 */
package com.metacube.shoppingcart.controller;

import java.util.List;
import java.util.Map;

import com.metacube.shoppingcart.facade.PerformCartOperations;
import com.metacube.shoppingcart.model.Product;
import com.metacube.shoppingcart.model.User;

public class Controller {
	
	/**
	 * Displays Stock
	 * @return stock
	 */
	public static List<Product> showStock(){
		return PerformCartOperations.showStock();
	}
	
	/**
	 * Adds Product to Cart
	 * @param productName
	 * @param quantity
	 */
	public static void addProductToCart(String productName,int quantity){
		PerformCartOperations.addProductToCart(productName,quantity);	
	}

	/**
	 * Deletes product from Cart
	 * @param productName
	 * @param quantity
	 */
	public static void deleteProductFromCart(String productName,int quantity){
		PerformCartOperations.deleteProductFromCart(productName,quantity);	
	}


	/**
	 * Displays Cart
	 * @return Cart Items
	 */
	public static Map<Product,Integer> showCart(){
		return PerformCartOperations.showCart();	
	}

	/**
	 * Displays Bill
	 * @return total Bill
	 */
	public static double showBill(){
		return PerformCartOperations.showBill();	
	}

	/**
	 * Displays User of a Cart
	 * @return user
	 */
	public static User showUser(){
		return PerformCartOperations.showUser();	
	}

	/**
	 * Adds User 
	 * @param user
	 */
	public static void setUser(User user){
		PerformCartOperations.setUser(user);	
	}
}