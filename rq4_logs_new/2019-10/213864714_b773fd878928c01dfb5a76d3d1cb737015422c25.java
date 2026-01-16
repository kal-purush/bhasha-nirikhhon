package com.springdemo.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.springdemo.entity.Customer;
import com.springdemo.service.CustomerService;

@Controller
@RequestMapping("/customer")
public class CustomerController {
   
	
	//need to inject customer Service layer
	
	@Autowired
	private CustomerService customerService;
	
	
	@RequestMapping("/list")
	public String listCustomers(Model theModel) {
		
		// get customers from Service
		List<Customer> theCustomers = customerService.getCustomers();
		
		//add customers to the model
		
		theModel.addAttribute("customers",theCustomers);
		
		return "list-customers";
	}
	
	@RequestMapping("/showFormForAdd")
	public String customerAddForm(Model theModel) {
		Customer theCustomer = new Customer();
		
		theModel.addAttribute("customer", theCustomer);
		
		return "customer-form";
	}
	
	@PostMapping("/saveCustomer")
	public String saveCustomer(@ModelAttribute("customer")Customer theCustomer) {
		customerService.saveCustomer(theCustomer);
		
		return "redirect:/customer/list";
	}
	
	@GetMapping("/showFormUpdate")
	public String updateForm(@RequestParam("customerId") int theId, Model theModel) {
		Customer theCustomer = customerService.getCustomer(theId);
		
		theModel.addAttribute("customer",theCustomer);
				
		return "customer-form";
	}
	
	@GetMapping("/delete")
	public String deleteCustomer(@RequestParam("customerId") int theId, Model theModel) {
		customerService.deleteCustomer(theId);
		return "redirect:/customer/list";
	}
	
	
}