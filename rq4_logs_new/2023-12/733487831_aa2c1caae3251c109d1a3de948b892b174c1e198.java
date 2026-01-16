package com.calculator.calculatordemo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CalculatorController {
    private final CalculatorService calculatorService;

    public CalculatorController(CalculatorService calculatorService) {
        this.calculatorService = calculatorService;
    }

    @GetMapping(path = "/calculator")
    public String answerWelcom() {
        return calculatorService.answerWelcom();
    }

    @GetMapping(path = "/calculator/plus")
    public String calculatorSum(@RequestParam(required = true) int num1, @RequestParam(required = true) int num2) {
        return calculatorService.calculatorSum(num1, num2);
    }

    @GetMapping(path = "/calculator/minus")
    public String calculatorSubtraction(@RequestParam(required = true) int num1, @RequestParam(required = true) int num2) {
        return calculatorService.calculatorSubtraction(num1, num2);
    }

    @GetMapping(path = "/calculator/multiply")
    public String calculatorMultiply(@RequestParam(required = true) int num1, @RequestParam(required = true) int num2) {
        return calculatorService.calculatorMultiply(num1, num2);
    }

    @GetMapping(path = "/calculator/divide")
    public String calculatorDivide(@RequestParam(required = true) int num1, @RequestParam(required = true) int num2) {
        return calculatorService.calculatorDivide(num1, num2);
    }

}
