package com.coe.andaman.cs.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.coe.andaman.cs.entity.Customer;
import com.coe.andaman.cs.service.CustomerService;

@RestController
@RequestMapping("/cs")
public class CustomerServiceController {
	@Autowired
	@Qualifier("CustomerService")
	CustomerService customerService;
	@GetMapping("/customers")
	ResponseEntity<?> getAllCustomer()
	{
		return new ResponseEntity<List<Customer>>(customerService.getAllCustomer(),HttpStatus.OK);
	}
	@GetMapping("/customers/{id}")
	ResponseEntity<?> getCustomerById(@PathVariable("id") String id) throws Exception
	{
		Customer customer=customerService.getCutomerById(Long.parseLong(id)).orElseThrow(()->new Exception("Customer Not found for id "+id));

		return new ResponseEntity<>(customer,HttpStatus.OK);
	}
	

}