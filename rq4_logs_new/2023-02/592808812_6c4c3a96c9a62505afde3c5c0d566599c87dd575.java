package com.revature.services;

import com.revature.models.Employee;
import com.revature.repositories.EmployeeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EmployeeService {

    private EmployeeRepository employeeRepository;

    @Autowired
    public EmployeeService(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
    }

    public Employee getEmployeeById(int empId)
    {
        return employeeRepository.findById(empId).orElse(null);
    }
    findRatingsByEmployeeId()

}