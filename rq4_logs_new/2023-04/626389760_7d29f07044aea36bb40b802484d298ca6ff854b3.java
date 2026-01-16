package com.it.academy.mortgage.calculator.controllers;

import com.it.academy.mortgage.calculator.dto.CalculateFormDto;
import com.it.academy.mortgage.calculator.dto.CalculateResultsDto;
import com.it.academy.mortgage.calculator.dto.DetailedFormDto;
import com.it.academy.mortgage.calculator.services.CalculatorService;
import com.it.academy.mortgage.calculator.services.DetailedCalculatorService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("calculate/detailed")
@CrossOrigin(origins = {"http://localhost:4200", "https://mortgage-loan-calculator-front-end2.onrender.com"}, allowCredentials="true")
public class DetailedCalculatorController {

    private final CalculatorService calculatorService;
    private final DetailedCalculatorService detailedCalculatorService;

    public DetailedCalculatorController(CalculatorService calculatorService, DetailedCalculatorService detailedCalculatorService) {
        this.calculatorService = calculatorService;
        this.detailedCalculatorService = detailedCalculatorService;
    }

    @PostMapping
    public CalculateResultsDto sendData(@Valid @RequestBody CalculateFormDto calculateFormDto) {
        return detailedCalculatorService.saveLoanDetailsAndResults(calculateFormDto);
//        return calculatorService.saveLoanDetailsAndResults(calculateFormDto);
    }

    @GetMapping("/forms")
    public List<DetailedFormDto> getAllForms() {
        return detailedCalculatorService.getDetailedDto();
    }


    @DeleteMapping("/form/{id}")
    public void deleteForm(@PathVariable(name = "id") Long id) {
        detailedCalculatorService.deleteDetailedForm(id);
    }
}