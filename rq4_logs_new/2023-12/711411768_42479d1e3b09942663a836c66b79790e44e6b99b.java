package com.h33kz.WarehouseManagementSystem.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.h33kz.WarehouseManagementSystem.models.Customer;
import com.h33kz.WarehouseManagementSystem.service.CustomerService;

@RestController
public class CustomerController {
  @Autowired
  private CustomerService customerService;

  @PostMapping
  public Customer insertCustomer(@RequestBody Customer customer){
    return customerService.insertCustomer(customer);
  }
  
}