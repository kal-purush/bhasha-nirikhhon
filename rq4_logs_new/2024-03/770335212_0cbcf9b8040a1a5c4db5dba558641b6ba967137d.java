package com.ictak.expensetrackerbe.controllers;

import com.ictak.expensetrackerbe.dbmodels.CategoryEntity;
import com.ictak.expensetrackerbe.dbmodels.PaymentTypeEntity;
import com.ictak.expensetrackerbe.repository.CategoryRepository;
import com.ictak.expensetrackerbe.repository.PaymentTypeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class ExpenseController {

    @Autowired
    private CategoryRepository categoryRepository;

    @Autowired
    private PaymentTypeRepository paymentTypeRepository;

    @CrossOrigin(origins = "http://localhost:3000")
    @GetMapping("/categories")
    public ResponseEntity<Map<String, Object>> getCategories() {
        Map<String, Object> response = new HashMap<>();
        try{
            List<CategoryEntity> result = categoryRepository.getCategories();
            if (result != null) {
                response.put("status", "success");
                response.put("code", 200);
                response.put("categories", result);
            } else {
                response.put("code", "404");
                response.put("categories", new ArrayList<>());
            }
        } catch (Exception e){
            response.put("status", "error");
            response.put("code", 500);
            response.put("message", e.getMessage());
        }
        return ResponseEntity.ok(response);
    }

    @CrossOrigin(origins = "http://localhost:3000")
    @GetMapping("/paymentTypes")
    public ResponseEntity<Map<String, Object>> getPaymentTypes() {
        Map<String, Object> response = new HashMap<>();
        try{
            List<PaymentTypeEntity> result = paymentTypeRepository.getPaymentTypes();
            if (result != null) {
                response.put("status", "success");
                response.put("code", 200);
                response.put("paymentTypes", result);
            } else {
                response.put("code", "404");
                response.put("paymentTypes", new ArrayList<>());
            }
        } catch (Exception e){
            response.put("status", "error");
            response.put("code", 500);
            response.put("message", e.getMessage());
        }
        return ResponseEntity.ok(response);
    }
}




