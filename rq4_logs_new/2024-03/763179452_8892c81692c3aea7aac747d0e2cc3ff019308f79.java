package com.BackendChallenge.TechTrendEmporium.controller;

import com.BackendChallenge.TechTrendEmporium.dto.JobDTO;
import com.BackendChallenge.TechTrendEmporium.dto.ReviewJobRequestDTO;
import com.BackendChallenge.TechTrendEmporium.service.AdminReviewService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class AdminReviewController {

    @Autowired
    private AdminReviewService adminReviewService;

    @GetMapping("/jobs")
    public ResponseEntity<Map<String, List<JobDTO>>> getPendingApprovalJobs() {
        Map<String, List<JobDTO>> response = new HashMap<>();
        response.put("jobs", adminReviewService.getPendingApprovalJobs());
        return ResponseEntity.ok(response);
    }
    @PostMapping("/reviewJob")
    public ResponseEntity<Map<String, String>> reviewJob(@RequestParam String type, @RequestBody ReviewJobRequestDTO requestDTO) {
        adminReviewService.reviewJob(type, requestDTO);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Success Approval");
        return ResponseEntity.ok(response);
    }
}
