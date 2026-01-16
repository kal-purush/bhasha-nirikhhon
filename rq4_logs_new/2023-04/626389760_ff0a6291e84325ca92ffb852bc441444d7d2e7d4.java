package com.it.academy.mortgage.calculator.controllers;

import com.it.academy.mortgage.calculator.dto.MonthlyPaymentDto;
import com.it.academy.mortgage.calculator.dto.MonthlyResultsDto;
import com.it.academy.mortgage.calculator.exceptions.CalculatorException;
import com.it.academy.mortgage.calculator.services.MonthlyPaymentService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController()
@RequestMapping("monthly-payment")
@CrossOrigin(origins = {"http://localhost:4200", "https://mortgage-loan-calculator-front-end2.onrender.com"})
public class MonthlyPaymentController {

    private final MonthlyPaymentService monthlyPaymentService = new MonthlyPaymentService();

    @PostMapping
    public ResponseEntity<MonthlyPaymentDto> getFormData(@RequestBody MonthlyPaymentDto monthlyPaymentDto) {
        return ResponseEntity.ok(monthlyPaymentDto);
    }

    @GetMapping()
    public MonthlyResultsDto sendFormData(@RequestBody MonthlyPaymentDto monthlyPaymentDto) {

        MonthlyResultsDto monthlyResultsDto = new MonthlyResultsDto();
        try {

            monthlyResultsDto.setEstimatedMonthlyPayment(monthlyPaymentService.estimatedMonthlyPayment(monthlyPaymentDto));
            monthlyResultsDto.setMaxMonthlyPayment(monthlyPaymentService.maxMonthlyPayment(monthlyPaymentDto));
        } catch (IOException exception) {
            throw new CalculatorException();
        }

        return monthlyResultsDto;
    }
}