package com.project.quanlycanghangkhong.controller;

import com.project.quanlycanghangkhong.dto.DocumentDTO;
import com.project.quanlycanghangkhong.service.TaskDocumentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/task-documents")
public class TaskDocumentController {
    @Autowired
    private TaskDocumentService taskDocumentService;

    @PostMapping("/attach")
    public ResponseEntity<DocumentDTO> attachDocument(@RequestParam Integer taskId, @RequestBody DocumentDTO documentDTO) {
        return ResponseEntity.ok(taskDocumentService.attachDocumentToTask(taskId, documentDTO));
    }

    @DeleteMapping("/remove")
    public ResponseEntity<Void> removeDocument(@RequestParam Integer taskId, @RequestParam Integer documentId) {
        taskDocumentService.removeDocumentFromTask(taskId, documentId);
        return ResponseEntity.ok().build();
    }

    @PutMapping("/update")
    public ResponseEntity<DocumentDTO> updateDocument(@RequestParam Integer taskId, @RequestParam Integer documentId, @RequestBody DocumentDTO documentDTO) {
        return ResponseEntity.ok(taskDocumentService.updateDocumentInTask(taskId, documentId, documentDTO));
    }
}