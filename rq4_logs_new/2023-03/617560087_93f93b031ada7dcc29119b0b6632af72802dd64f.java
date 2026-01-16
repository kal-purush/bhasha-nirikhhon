package com.springboot_backend.springboot_backend_thymeleaf.controller;

import com.springboot_backend.springboot_backend_thymeleaf.model.Employee;
import com.springboot_backend.springboot_backend_thymeleaf.service.imp.EmployeeServiceImp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Controller
@RequestMapping("api/employee")
public class Employee_Controller {
    @Autowired
    private final EmployeeServiceImp employeeServiceImp;

    public Employee_Controller( EmployeeServiceImp employeeServiceImp){
        this.employeeServiceImp = employeeServiceImp;
    }


    //display list of employee
    @GetMapping()
    public String homeViewPage(Model model) {
        model.addAttribute("listEmployees", employeeServiceImp.getAllEmployee());
        return "index";
    }

 /*
    @GetMapping("")
    public List<Employee> getAllEmployees(){
        return employeeServiceImp.getAllEmployee();
    }

  */
}