package com.capstone.dentalclinic.demo.controller;

import com.capstone.dentalclinic.demo.services.EmployeeService;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@AllArgsConstructor
@RequestMapping("/token")
public class Token {

    private final EmployeeService employeeService;


    @GetMapping("/confirm")
    public String confirmToken(@RequestParam("token") String token) {
        return employeeService.confirmTokens(token);
    }
}