package com.project.quanlycanghangkhong.controller;

import com.project.quanlycanghangkhong.dto.EvaluationIssueDTO;
import com.project.quanlycanghangkhong.service.EvaluationIssueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/evaluation-issues")
public class EvaluationIssueController {
    @Autowired
    private EvaluationIssueService evaluationIssueService;

    @GetMapping
    public ResponseEntity<List<EvaluationIssueDTO>> getAllIssues() {
        return ResponseEntity.ok(evaluationIssueService.getAllIssues());
    }

    @GetMapping("/session/{sessionId}")
    public ResponseEntity<List<EvaluationIssueDTO>> getIssuesBySession(@PathVariable Integer sessionId) {
        return ResponseEntity.ok(evaluationIssueService.getIssuesBySession(sessionId));
    }

    @GetMapping("/{id}")
    public ResponseEntity<EvaluationIssueDTO> getIssueById(@PathVariable Integer id) {
        EvaluationIssueDTO dto = evaluationIssueService.getIssueById(id);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dto);
    }

    @PostMapping
    public ResponseEntity<EvaluationIssueDTO> createIssue(@RequestBody EvaluationIssueDTO dto) {
        EvaluationIssueDTO created = evaluationIssueService.createIssue(dto);
        return ResponseEntity.ok(created);
    }

    @PutMapping("/{id}")
    public ResponseEntity<EvaluationIssueDTO> updateIssue(@PathVariable Integer id, @RequestBody EvaluationIssueDTO dto) {
        EvaluationIssueDTO updated = evaluationIssueService.updateIssue(id, dto);
        if (updated == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(updated);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteIssue(@PathVariable Integer id) {
        evaluationIssueService.deleteIssue(id);
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/{id}/status")
    public ResponseEntity<EvaluationIssueDTO> updateIssueStatus(@PathVariable Integer id, @RequestBody StatusUpdateRequest request) {
        EvaluationIssueDTO updated = evaluationIssueService.updateIssueStatus(id, request.getIsResolved(), request.getResolutionDate());
        if (updated == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(updated);
    }

    public static class StatusUpdateRequest {
        private Boolean isResolved;
        private java.time.LocalDate resolutionDate;
        public Boolean getIsResolved() { return isResolved; }
        public void setIsResolved(Boolean isResolved) { this.isResolved = isResolved; }
        public java.time.LocalDate getResolutionDate() { return resolutionDate; }
        public void setResolutionDate(java.time.LocalDate resolutionDate) { this.resolutionDate = resolutionDate; }
    }
}