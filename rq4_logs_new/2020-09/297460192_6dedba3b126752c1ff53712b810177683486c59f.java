package com.shoppingapp;

import java.util.ArrayList;
import java.util.List;

import com.shoppingapp.controller.ShoppingController;
import com.shoppingapp.model.Customer;
import com.shoppingapp.model.Item;
import com.shoppingapp.utility.ConsolePrinterUtility;

public class ShoppingApp
{
	
	
	public static void main(String[] args)
	{
		ShoppingController shoppingController = new ShoppingController();
		shoppingController.doShopping();
		//testMethods();
	}
	
	public static void testMethods()
	{
		// Testing methods/Models
		Customer cust = new Customer("Josh", "pass");
		System.out.println(cust.toString());
		Item item = new Item("Name of item", "J01", 20.20);
		System.out.println(item.toString());
		
		ConsolePrinterUtility cpu = new ConsolePrinterUtility();
		cpu.displayCustomerInfo(cust);
		cpu.mainMenu();
		//cpu.shop();
		cpu.login();
//		for(Item tem : inventory)
//		{
//			System.out.println(tem.toString());
//		}
	}

}