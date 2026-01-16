package com.rh.Internship.employee.services;

import com.rh.Internship.employee.Employee;
import com.rh.Internship.employee.SalariedEmployee;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Saeed Zarinfam
 */
public class SalaryServiceWithTax implements SalaryService {

    @Override
    public float calculateSalary(Employee employee) {
        float taxRate = employee instanceof SalariedEmployee ? 0.9F: 0.95F;
        return employee.calculateSalary() * taxRate;
    }

    @Override
    public Map<String, Float> calculateBatchSalary(List<Employee> employeeList) {
        Map<String, Float> salaryResult = new HashMap<>();

        employeeList.forEach(employee -> salaryResult.put(employee.getCode(), calculateSalary(employee)));

        return salaryResult;
    }
}