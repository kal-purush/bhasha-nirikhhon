package com.example.timetracker.controller;

import com.example.timetracker.dto.SummaryDTO;
import com.example.timetracker.service.SummaryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/users/{userId}/summary")
public class SummaryController {

    @Autowired
    private SummaryService summaryService;

    @GetMapping
    public SummaryDTO getSummary(@PathVariable UUID userId) {
        return summaryService.createSummary(userId);
    }
}