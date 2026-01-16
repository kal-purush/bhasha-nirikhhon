package com.example.policybazaar.com.customer.Controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.policybazaar.com.customer.Entity.Customer;
import com.example.policybazaar.com.customer.Services.CustomerService;

@RestController
@RequestMapping("/customer")
public class CustomerController {
	@Autowired private CustomerService services;
	/*
	 * @Autowired Producer producer;
	 * 
	 * 
	 * 
	 * @GetMapping("/producerMsg") public void
	 * getMessageFromClient(@RequestParam("message") String message) {
	 * producer.sendMsgToTopic(message); }
	 */

	@PostMapping("/addCustomer")
	public ResponseEntity<Map<String, Object>> addCustomer(@RequestBody Customer customer) {
		Map<String, Object> response = new HashMap<>();
		try {
			Customer cust = services.saveCustomer(customer);

			if (cust != null) {
				response.put("message", "Customer is succesfully added");
			} else {
				response.put("message", "Something went wrong !Please try again");
			}
		} catch (Exception e) {
			// TODO: handle exception
			response.put("message", "Something went wrong !Please try again");
		}

		return new ResponseEntity<Map<String, Object>>(response, HttpStatus.OK);
	}
	@PostMapping("/updateCustomer")
	public ResponseEntity<Map<String, Object>> updateCustomer(@RequestBody Customer customer) {
		Map<String, Object> response = new HashMap<>();
		try {
			Customer cust = services.updateCustomer(customer);

			if (cust != null) {
				response.put("message", "Customer is succesfully updated");
			} else {
				response.put("message", "Something went wrong !Please try again");
			}
		} catch (Exception e) {
			
		}
		return new ResponseEntity<Map<String, Object>>(response, HttpStatus.OK);
	}
	
	@GetMapping("/getCustomerById/{id}")
	public ResponseEntity<Map<String, Object>> updateCustomer(@PathVariable int id) {
		Map<String, Object> response = new HashMap<>();
		try {
			Customer cust = services.getCustomerByCustId(id);

			if (cust != null) {
				response.put("message", "Customer Data fetched");
				response.put("data", cust);
			} else {
				response.put("message", "Customer is not present .Please add data!");
			}
		} catch (Exception e) {
			
		}
		return new ResponseEntity<Map<String, Object>>(response, HttpStatus.OK);
	} 
}