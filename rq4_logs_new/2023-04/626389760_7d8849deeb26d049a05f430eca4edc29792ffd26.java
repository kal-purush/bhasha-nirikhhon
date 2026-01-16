package com.it.academy.mortgage.calculator.controllers;

import com.it.academy.mortgage.calculator.dto.CalculateFormDto;
import com.it.academy.mortgage.calculator.services.CalculateFormService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/calculate/form")
@CrossOrigin(origins = {"http://localhost:4200", "https://mortgage-loan-calculator-front-end2.onrender.com"})
public class CalculateFormController {

    private CalculateFormService calculateFormService;

    public CalculateFormController(CalculateFormService calculateFormService) {
        this.calculateFormService = calculateFormService;
    }

    @GetMapping("/{id}")
    public CalculateFormDto findById (@PathVariable(name = "id") Long id){
        return calculateFormService.findById(id);
    }
    @GetMapping("")
    public List<CalculateFormDto> getAllForms (){
        return calculateFormService.getAllLoanDetailsList();
    }
    @PostMapping("")
    public ResponseEntity<CalculateFormDto> saveLoanFormDetails (@RequestBody CalculateFormDto calculateFormDto) {
        calculateFormService.saveLoanDetails(calculateFormDto);
        return ResponseEntity.ok(calculateFormDto);
    }
    @DeleteMapping("{id}")
    public void deletePatient (@PathVariable (name = "id") Long id){
        calculateFormService.deleteCalculateForm(id);
    }
}