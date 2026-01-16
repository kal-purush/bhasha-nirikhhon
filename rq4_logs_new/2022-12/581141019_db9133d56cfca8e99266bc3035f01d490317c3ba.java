package com.ems.ThymeLeaf.service;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ems.ThymeLeaf.entity.Employee;
import com.ems.ThymeLeaf.repository.EmployeeRepo;

@Service
public class employeeServiceImpl implements employeeService{

	@Autowired
	private EmployeeRepo employeeRepository;

	@Override
	public void saveEmployee(Employee emp) {
		// TODO Auto-generated method stub
		this.employeeRepository.save(emp);
	}

	@Override
	public Employee getEmployeeById(int eid) {
		// TODO Auto-generated method stub
		Optional<Employee> optional = employeeRepository.findById(eid);
		Employee employee = null;
		if(optional.isPresent()) {
			employee = optional.get();
		}else {
			throw new RuntimeException("Employee not found");
		}
		return employee;
	}

	@Override
	public void deleteEmployeeById(int eid) {
		// TODO Auto-generated method stub
		this.employeeRepository.deleteById(eid);
	}

}