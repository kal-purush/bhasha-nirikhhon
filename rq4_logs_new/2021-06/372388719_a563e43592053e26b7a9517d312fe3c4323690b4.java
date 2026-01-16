package com.org.loan.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.org.common.exception.BankManagementSystemException;
import com.org.common.handlers.ResponseHandlers;
import com.org.common.model.ServiceResponse;
import com.org.loan.model.Customer;
import com.org.loan.model.Loan;
import com.org.loan.service.ICustomerService;

@RestController
@CrossOrigin
@RequestMapping("customer")
public class CustomerController {
	@Autowired
	private ICustomerService customerService;
	@PostMapping("/updateCustomer")
	public ResponseEntity<ServiceResponse<Customer>>  customerUpdate(@RequestBody Customer customer) throws BankManagementSystemException  {
		if(customer !=null) {
		 customerService.createCustomer(customer);
		} else {
			throw new BankManagementSystemException("Not Inserted");
		}
		return new ResponseHandlers<String>().defaultResponse("Customer created successfully.", HttpStatus.CREATED);
	}
	
	@PostMapping("/applyLoans")
	public ResponseEntity<ServiceResponse<Loan>> loan( @RequestBody Loan loan) throws BankManagementSystemException {
		if (loan != null) {
			customerService.applyLoan(loan);
		}else {
			throw new BankManagementSystemException("Not Inserted");
		}
		return new ResponseHandlers<String>().defaultResponse("Loan Applied successfully.", HttpStatus.CREATED);
	}
}