package com.fptgang.backend.controller;

import com.fptgang.backend.service.StatService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/stats")
public class StatController {

    private final StatService statService;

    public StatController(StatService statService) {
        this.statService = statService;
    }

    /**
     * Get daily order count as time-series data
     * @return List of time-series data [{x: date, y: orderCount}]
     */
    @GetMapping("/daily-orders")
    public ResponseEntity<List<Map<String, Object>>> getDailyOrders() {
        log.info("Fetching daily order statistics...");
        List<Map<String, Object>> response = statService.getDailyOrders();
        return ResponseEntity.ok(response);
    }
    /**
     * Get daily revenue as time-series data
     * @return List of time-series data [{x: date, y: totalRevenue}]
     */
    @GetMapping("/daily-revenue")
    public ResponseEntity<List<Map<String, Object>>> getDailyRevenue() {
        log.info("Fetching daily revenue statistics...");
        List<Map<String, Object>> response = statService.getDailyRevenue();
        return ResponseEntity.ok(response);
    }

    /**
     * Get monthly revenue as time-series data
     * @return List of time-series data [{x: yyyy-MM, y: totalRevenue}]
     */
    @GetMapping("/monthly-revenue")
    public ResponseEntity<List<Map<String, Object>>> getMonthlyRevenue() {
        log.info("Fetching monthly revenue statistics...");
        List<Map<String, Object>> response = statService.getMonthlyRevenue();
        return ResponseEntity.ok(response);
    }

        @GetMapping("/daily-new-customers")
    public ResponseEntity<List<Map<String, Object>>> getDailyNewCustomers() {
        log.info("Fetching daily new customers statistics...");
        List<Map<String, Object>> response = statService.getDailyNewCustomers();
        return ResponseEntity.ok(response);
    }

    @GetMapping("/monthly-new-customers")
    public ResponseEntity<List<Map<String, Object>>> getMonthlyNewCustomers() {
        log.info("Fetching monthly new customers statistics...");
        List<Map<String, Object>> response = statService.getMonthlyNewCustomers();
        return ResponseEntity.ok(response);
    }
}