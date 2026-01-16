package com.project.quanlycanghangkhong.controller;

import com.project.quanlycanghangkhong.dto.AssignmentDTO;
import com.project.quanlycanghangkhong.service.AssignmentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/assignments")
public class AssignmentController {
    @Autowired
    private AssignmentService assignmentService;

    // Giao công việc (tạo mới assignment)
    @PostMapping
    public ResponseEntity<AssignmentDTO> createAssignment(@RequestBody AssignmentDTO dto) {
        return ResponseEntity.ok(assignmentService.createAssignment(dto));
    }

    // Cập nhật giao công việc
    @PutMapping("/{id}")
    public ResponseEntity<AssignmentDTO> updateAssignment(@PathVariable Integer id, @RequestBody AssignmentDTO dto) {
        return ResponseEntity.ok(assignmentService.updateAssignment(id, dto));
    }

    // Xoá giao công việc
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteAssignment(@PathVariable Integer id) {
        assignmentService.deleteAssignment(id);
        return ResponseEntity.noContent().build();
    }

    // Xem chi tiết giao công việc
    @GetMapping("/{id}")
    public ResponseEntity<AssignmentDTO> getAssignmentById(@PathVariable Integer id) {
        return ResponseEntity.ok(assignmentService.getAssignmentById(id));
    }

    // Lấy danh sách giao công việc theo task
    @GetMapping("/task/{taskId}")
    public ResponseEntity<List<AssignmentDTO>> getAssignmentsByTaskId(@PathVariable Integer taskId) {
        return ResponseEntity.ok(assignmentService.getAssignmentsByTaskId(taskId));
    }
}