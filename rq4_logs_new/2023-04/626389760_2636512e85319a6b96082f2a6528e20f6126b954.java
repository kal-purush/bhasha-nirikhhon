package com.it.academy.mortgage.calculator.services;

import com.it.academy.mortgage.calculator.exceptions.CustomerNotFoundException;
import com.it.academy.mortgage.calculator.models.Customer;
import com.it.academy.mortgage.calculator.repos.CustomerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class CustomerService {
    private final CustomerRepository customerRepository;

    public CustomerService(CustomerRepository customerRepository) {
        this.customerRepository = customerRepository;
    }

    public List<Customer> fetchAllCustomers() {return customerRepository.findAll();}

    public void addCustomer(Customer newCustomer) {customerRepository.save(newCustomer);}

    public void deleteCustomerById(String id) {customerRepository.deleteById(id);}

    public List<Customer> fetchCustomersByName(String name){
        return customerRepository.findAll().stream().filter(customer -> Objects.equals(customer.getName(), name)).collect(Collectors.toList());
    };

    public Customer fetchCustomerById(String id) {
        return customerRepository.findById(id).orElseThrow(()->new CustomerNotFoundException(id));
    }
}