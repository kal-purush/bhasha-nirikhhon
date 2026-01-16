package com.mertnamsal.controller;

import java.util.Arrays;

import com.mertnamsal.entity.Customer;
import com.mertnamsal.entity.Product;
import com.mertnamsal.service.CustomerService;
import com.mertnamsal.service.ProductService;
import com.mertnamsal.util.HibernateUtils;


public class Test {

	public static void main(String[] args) {
		HibernateUtils.getSessionFactory().openSession();
		CustomerService c = new CustomerService();
		ProductService p = new ProductService();
		
//		Product product=p.getById(1);
//		Product product = new Product("Samsung s20",15000,10);
//		p.save(product);
//		
//		
//		Customer cust = new Customer("Ahmet", "Kaya", "ahmetk@gmail.com", "123ahmet", 12345674,Arrays.asList(product));
//		
//		
//		
//		c.save(cust);
		
//		Customer cust = new Customer("Ahmet", "Kaya", "ahmetk@gmail.com", "123ahmet", 12345674);
//		c.save(cust);
//		
//		Product product = new Product("Samsung s20",15000,10);
//		
//		p.save(product);
//		
//		cust.setProducts(Arrays.asList(product));
//		c.update(cust);
		
		//yöntemmm
		
	}
}