package com.example.backend.controllers;

import com.example.backend.dtos.DashboardDTO;
import com.example.backend.services.DashboardService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/dashboard")
@RequiredArgsConstructor
public class DashboardController {
    private final DashboardService dashboardService;

    @GetMapping("/getCount")
    public ResponseEntity<DashboardDTO> getCount() {
        return ResponseEntity.ok(dashboardService.getDashboard());
    }
}