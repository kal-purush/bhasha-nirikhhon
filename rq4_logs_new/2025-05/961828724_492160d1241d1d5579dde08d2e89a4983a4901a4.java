package com.project.quanlycanghangkhong.controller;

import com.project.quanlycanghangkhong.dto.AttachmentDTO;
import com.project.quanlycanghangkhong.service.AttachmentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/attachments")
public class AttachmentController {
    @Autowired
    private AttachmentService attachmentService;

    @PostMapping("/document/{documentId}")
    public ResponseEntity<AttachmentDTO> addAttachment(@PathVariable Integer documentId, @RequestBody AttachmentDTO dto) {
        return ResponseEntity.ok(attachmentService.addAttachmentToDocument(documentId, dto));
    }

    @PutMapping("/{id}")
    public ResponseEntity<AttachmentDTO> updateAttachment(@PathVariable Integer id, @RequestBody AttachmentDTO dto) {
        return ResponseEntity.ok(attachmentService.updateAttachment(id, dto));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteAttachment(@PathVariable Integer id) {
        attachmentService.deleteAttachment(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/document/{documentId}")
    public ResponseEntity<List<AttachmentDTO>> getAttachmentsByDocument(@PathVariable Integer documentId) {
        return ResponseEntity.ok(attachmentService.getAttachmentsByDocumentId(documentId));
    }
}