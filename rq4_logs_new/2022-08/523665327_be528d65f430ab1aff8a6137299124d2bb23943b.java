package com.example.springbootcalculator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@CrossOrigin
@RestController
@RequestMapping("api")
public class CalculatorController{
    @Autowired
    private CalculatorService calculatorService;

    @PostMapping
    public double add(int a, int b) {
        return 0;
    }

    @PostMapping
    public double subtract(int a, int b) {
        return 0;
    }

    @PostMapping
    public double multiply(int a, int b) {
        return 0;
    }

    @PostMapping
    public double divide(int a, int b) {
        return 0;
    }
}