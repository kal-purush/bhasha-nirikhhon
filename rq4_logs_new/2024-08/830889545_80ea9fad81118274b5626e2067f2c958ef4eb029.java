package com.example.calculator.controller;

import com.example.calculator.model.Calculation;
import com.example.calculator.service.CalculationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class CalculatorController {

    @Autowired
    private CalculationService calculationService;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String showForm() {
        return "calculator";
    }

    @RequestMapping(value = "/calculate", method = RequestMethod.POST)
    public String calculate(@RequestParam("num1") double num1,
                            @RequestParam("num2") double num2,
                            @RequestParam("operation") String operation,
                            Model model) {
        double result = calculationService.calculate(num1, num2, operation);
        model.addAttribute("num1", num1);
        model.addAttribute("num2", num2);
        model.addAttribute("operation", operation);
        model.addAttribute("result", result);
        return "calculator";
    }
}