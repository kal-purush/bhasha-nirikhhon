package com.mamoni.tutorials.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;  
import org.springframework.stereotype.Controller;  
import org.springframework.web.bind.annotation.ModelAttribute;  
import org.springframework.web.bind.annotation.RequestMapping;  
import org.springframework.web.bind.annotation.RequestParam;  
import org.springframework.web.servlet.ModelAndView;  

import com.mamoni.tutorials.model.Employee;
import com.mamoni.tutorials.services.EmployeeService;

@Controller
public class EmployeeController {
	
	@Autowired
	private EmployeeService employeeService;
	
	@RequestMapping(value="/form")
	public ModelAndView getForm(@ModelAttribute Employee employee){
		return new ModelAndView("form");
	}
	
	@RequestMapping(value="/list")
	public ModelAndView getList(){
		//System.out.println("In getting employee list");
		List<Employee> employeeList = employeeService.getList();  
		//System.out.println("After getting employee list");
		for(Employee emp:employeeList){
			System.out.println("Employee name: " + emp.getFirstName());
		}
		return new ModelAndView("list", "employeeList", employeeList);  
	}
	
	@RequestMapping(value="/delete")
	public ModelAndView deleteEmployee(@RequestParam int id){
		employeeService.deleteRow(id);
		return new ModelAndView("redirect:list");
	}
	
	@RequestMapping("/edit")  
	 public ModelAndView editUser(@RequestParam int id, @ModelAttribute Employee employee) {  
	  Employee employeeObject = employeeService.getRowById(id);  
	  return new ModelAndView("edit", "employeeObject", employeeObject);  
	 }  

	@RequestMapping("/update")  
	 public ModelAndView updateUser(@ModelAttribute Employee employee) {  
		employeeService.updateRow(employee);  
	  return new ModelAndView("redirect:list");  
	 }  
	
}