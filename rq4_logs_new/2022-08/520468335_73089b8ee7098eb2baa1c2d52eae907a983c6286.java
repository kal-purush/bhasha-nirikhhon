package com.example.demo.contoller;

import java.util.List;

import javax.management.RuntimeErrorException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.entity.Employee;
import com.example.demo.service.EmployeeService;


@RestController
@RequestMapping("/emp")
public class EmployeeContoller implements EmployeeContollerInterface
{
	@Autowired
	EmployeeService employeeService;
	
	@GetMapping("/show")
	@Override
	public List<Employee> findAll() {
		
		return employeeService.findAll() ;
	}
	
	@GetMapping("/find/{id}")
	@Override
	public Employee findById(@PathVariable int id) 
	{
		Employee e = employeeService.findById(id);
		if(e==null)
		{
			throw new RuntimeException( "EMP Id "+id+" details not found");
		}
		else
		{
			return e;
		}
	}

	@PostMapping("/save")
	@Override
	public Employee save(@RequestBody Employee e) 
	{
		e.setId(0);
		employeeService.save(e);
		return e;
	}

	@PutMapping("/update")
	@Override
	public Employee saveOrUpdate(@RequestBody Employee e) 
	{
		employeeService.saveOrUpdate(e); 
		return e;
	}

	@DeleteMapping("/delete/{id}")
	@Override
	public void deleteById(@PathVariable int id)
	{
		Employee e = findById(id);
		
		if(e==null)
		{
			throw new RuntimeException("Id is not avilable in the database");
		}
		else
		{
			employeeService.deleteById(id);
		}
	}
	
	
}