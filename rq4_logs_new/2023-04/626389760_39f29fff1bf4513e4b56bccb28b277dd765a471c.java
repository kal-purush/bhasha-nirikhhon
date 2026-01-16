package com.it.academy.mortgage.calculator.controllers;

import com.it.academy.mortgage.calculator.models.CalculateFormDto;
import com.it.academy.mortgage.calculator.services.CalculatorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController()
@RequestMapping("calculate")
public class CalculatorController {

    private final CalculatorService calculatorService = new CalculatorService();
    Logger logger = LoggerFactory.getLogger(CalculatorController.class);

    @PostMapping
    public ResponseEntity<CalculateFormDto> getFormData(@RequestBody CalculateFormDto formData) {
        return ResponseEntity.ok(formData);
    }

    @GetMapping()
    public CalculateFormDto sendFormData(@RequestParam("homePrice") int homePrice,
                                         @RequestParam("monthlyIncome") int monthlyIncome,
                                         @RequestParam("loanTerm") int loanTerm) {

        CalculateFormDto calculateFormDto = new CalculateFormDto(homePrice, monthlyIncome, loanTerm);
        try {

            calculateFormDto.setMaxLoan(calculatorService.maxLoan(calculateFormDto));
            calculateFormDto.setTotalInterestPaid(calculatorService.totalInterestPaid(calculateFormDto));
            calculateFormDto.setAgreementFee(calculatorService.agreementFee(calculateFormDto));
            calculateFormDto.setTotalPaymentSum(calculatorService.totalPaymentSum(calculateFormDto));
        } catch (IOException exception) {
            logger.error("Error: " + exception);
        }

        return calculateFormDto;
    }
}