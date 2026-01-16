package com.project.quanlycanghangkhong.controller;

import com.project.quanlycanghangkhong.dto.EvaluationSessionDTO;
import com.project.quanlycanghangkhong.service.EvaluationSessionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/evaluation-sessions")
public class EvaluationSessionController {
    @Autowired
    private EvaluationSessionService evaluationSessionService;

    @GetMapping
    public ResponseEntity<List<EvaluationSessionDTO>> getAllEvaluationSessions() {
        return ResponseEntity.ok(evaluationSessionService.getAllEvaluationSessions());
    }

    @GetMapping("/{id}")
    public ResponseEntity<EvaluationSessionDTO> getEvaluationSessionById(@PathVariable Integer id) {
        EvaluationSessionDTO dto = evaluationSessionService.getEvaluationSessionById(id);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dto);
    }

    @PostMapping
    public ResponseEntity<EvaluationSessionDTO> createEvaluationSession(@RequestBody EvaluationSessionDTO dto) {
        EvaluationSessionDTO created = evaluationSessionService.createEvaluationSession(dto);
        return ResponseEntity.ok(created);
    }

    @PutMapping("/{id}")
    public ResponseEntity<EvaluationSessionDTO> updateEvaluationSession(@PathVariable Integer id, @RequestBody EvaluationSessionDTO dto) {
        EvaluationSessionDTO updated = evaluationSessionService.updateEvaluationSession(id, dto);
        if (updated == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(updated);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteEvaluationSession(@PathVariable Integer id) {
        evaluationSessionService.deleteEvaluationSession(id);
        return ResponseEntity.noContent().build();
    }
}