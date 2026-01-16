package com.example.ais_v5.controllers;

import com.example.ais_v5.entity.Grade;
import com.example.ais_v5.entity.User;
import com.example.ais_v5.repositorys.GradeRepository;
import com.example.ais_v5.services.GradeService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Validated
public class GradeController {

    private final GradeService gradeService;

    @GetMapping("/grades")
    public List<Grade> getUserGrades() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();


        return gradeService.findGradesByUsername(authentication.getName());
    }

}