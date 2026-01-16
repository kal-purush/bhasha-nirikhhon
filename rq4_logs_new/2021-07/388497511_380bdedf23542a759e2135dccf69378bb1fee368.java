package edigital.nepal.emssolution.web;

import edigital.nepal.emssolution.api.v1.EmployeeAPI;
import edigital.nepal.emssolution.models.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.swing.*;
import java.util.List;

@Controller
public class EmployeeController {

    EmployeeAPI employeeAPI;

    @Autowired
    public EmployeeController(EmployeeAPI employeeAPI){
        this.employeeAPI = employeeAPI;
    }

    @RequestMapping(value = "index",method = RequestMethod.GET)
    @ModelAttribute("employees")
    public List<Employee> index(){

        System.out.print(employeeAPI.getAllEmployees());
        return employeeAPI.getAllEmployees();
    }
}