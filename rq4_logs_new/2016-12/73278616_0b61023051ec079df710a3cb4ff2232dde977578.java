package com.packt.ticketportal.domain.repository;

import com.packt.ticketportal.domain.Customer;

import java.util.List;

/**
 * Created by Kuba on 2016-12-05.
 */
public interface ICustomerRepository extends IRepository<Customer> {
    public List<Customer> withName(String name);

}