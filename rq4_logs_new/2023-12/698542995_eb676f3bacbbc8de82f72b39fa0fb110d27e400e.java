package com.example.backend.controller;

import com.example.backend.controller.response.SubjectResponseDTO;
import com.example.backend.service.SubjectService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/subjects")
@RequiredArgsConstructor
public class SubjectController {
    private final SubjectService subjectService;
    @GetMapping
    public List<SubjectResponseDTO> getAllSubjects() {
        return subjectService.getAllSubjects();
    }
}