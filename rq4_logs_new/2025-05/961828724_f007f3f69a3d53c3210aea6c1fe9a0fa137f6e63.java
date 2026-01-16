package com.project.quanlycanghangkhong.controller;

import com.project.quanlycanghangkhong.dto.EvaluationGroupDTO;
import com.project.quanlycanghangkhong.service.EvaluationGroupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/evaluation-groups")
public class EvaluationGroupController {
    @Autowired
    private EvaluationGroupService evaluationGroupService;

    @GetMapping
    public ResponseEntity<List<EvaluationGroupDTO>> getAllEvaluationGroups() {
        return ResponseEntity.ok(evaluationGroupService.getAllEvaluationGroups());
    }

    @GetMapping("/{id}")
    public ResponseEntity<EvaluationGroupDTO> getEvaluationGroupById(@PathVariable Integer id) {
        EvaluationGroupDTO dto = evaluationGroupService.getEvaluationGroupById(id);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dto);
    }

    @PostMapping
    public ResponseEntity<EvaluationGroupDTO> createEvaluationGroup(@RequestBody EvaluationGroupDTO evaluationGroupDTO) {
        EvaluationGroupDTO created = evaluationGroupService.createEvaluationGroup(evaluationGroupDTO);
        return ResponseEntity.ok(created);
    }

    @PutMapping("/{id}")
    public ResponseEntity<EvaluationGroupDTO> updateEvaluationGroup(@PathVariable Integer id, @RequestBody EvaluationGroupDTO evaluationGroupDTO) {
        EvaluationGroupDTO updated = evaluationGroupService.updateEvaluationGroup(id, evaluationGroupDTO);
        if (updated == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(updated);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteEvaluationGroup(@PathVariable Integer id) {
        evaluationGroupService.deleteEvaluationGroup(id);
        return ResponseEntity.noContent().build();
    }
}