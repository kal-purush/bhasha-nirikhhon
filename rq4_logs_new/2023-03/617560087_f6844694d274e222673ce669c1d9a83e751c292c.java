package com.springboot_backend.springboot_backend_thymeleaf.controller;

import com.springboot_backend.springboot_backend_thymeleaf.model.Employee;
import com.springboot_backend.springboot_backend_thymeleaf.service.imp.EmployeeServiceImp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/home")
public class HomeController {

    /**
     Here is an example of a employee addition flow.
     You access the form at localhost:8080/home
     Input all fields and press "Add Employee" button to add a new employee
     Press the "Show Employees" button to go to the list of employees
     */

    @Autowired
    private final EmployeeServiceImp employeeServiceImp;

    public HomeController(EmployeeServiceImp employeeServiceImp) {
        this.employeeServiceImp = employeeServiceImp;
    }

    @GetMapping
    public String showHomePage(Model model) {
        model.addAttribute("employee", new Employee());
        return "index.html";
    }

    @PostMapping
    public String addEmployee(@ModelAttribute("employee") Employee employee, Model model) {
        employeeServiceImp.addEmployee(employee);
        model.addAttribute("employee", new Employee());
        return "index.html";
    }


}