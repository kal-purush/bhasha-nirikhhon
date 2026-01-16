package com.ictak.expensetrackerbe.controllers;
import com.ictak.expensetrackerbe.repository.ExpenseRepository;
import com.ictak.expensetrackerbe.repository.IncomeRepository;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class DashboardController {
    @Autowired
    private ExpenseRepository expenseRepository;
    @Autowired
    private IncomeRepository incomeRepository;
    @CrossOrigin(origins = "http://localhost:3000", allowedHeaders = "*")
    @GetMapping(value = "/monthlysummary", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> getMonthlySummary(@RequestHeader(name = "Authorization") String token,
                                                                 @RequestParam int userId){
        Map<String, Object> response = new HashMap<>();
        try{
            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
            if (authentication.isAuthenticated()){
                JSONArray jsonArray = new JSONArray();
                Double expenseAmount = expenseRepository.getMonthlyExpense(userId);
                Double incomeAmount = incomeRepository.getMonthlyIncome(userId);
                JSONObject balanceObj = new JSONObject();
                balanceObj.put("title", "Balance");
                balanceObj.put("amount", (incomeAmount == null || expenseAmount == null) ?
                                                            0 : incomeAmount - expenseAmount);
                jsonArray.put(balanceObj);
                JSONObject incomeObj = new JSONObject();
                incomeObj.put("title", "Income");
                incomeObj.put("amount", incomeAmount == null ? 0 : incomeAmount);
                jsonArray.put(incomeObj);
                JSONObject expenseObj = new JSONObject();
                expenseObj.put("title", "Expense");
                expenseObj.put("amount", expenseAmount == null ? 0 : expenseAmount);
                jsonArray.put(expenseObj);
                response.put("status", "success");
                response.put("code", 200);
                response.put("summary", jsonArray.toString());
            }
            else {
                // User is not authenticated (token validation failed)
                response.put("status", "error");
                response.put("message", "Token validation failed");
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(response);
            }
        } catch (Exception e) {
            response.put("status", "error");
            response.put("code", 500);
            response.put("message", e.getMessage());
        }
        return ResponseEntity.ok(response);
    }
    @CrossOrigin(origins = "http://localhost:3000", allowedHeaders = "*")
    @GetMapping(value = "/categoricalexpenses")
    public ResponseEntity<Map<String, Object>> getCategorieExpenses(@RequestHeader(name = "Authorization") String token,
                                                                    @RequestParam int userId){
        Map<String, Object> response = new HashMap<>();
        try{
            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
            if(authentication.isAuthenticated()){
                List<Map<String, Object>> result = expenseRepository.getCategoricalExpense(userId);
                if(result != null) {
                    response.put("status", "success");
                    response.put("code", 200);
                    response.put("categoricalExpenses", result);
                } else {
                    response.put("status", "not available");
                    response.put("code", 404);
                    response.put("categoricalExpenses", new ArrayList<>());
                }
            } else {
                response.put("status", "error");
                response.put("message", "Token validation failed");
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(response);
            }
        } catch (Exception e){
            response.put("status", "error");
            response.put("code", 500);
            response.put("message", e.getMessage());
        }
        return ResponseEntity.ok(response);
    }
}