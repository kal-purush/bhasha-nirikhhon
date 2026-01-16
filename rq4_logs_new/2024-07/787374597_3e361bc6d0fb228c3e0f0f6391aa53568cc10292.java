package com.example.moreveiw.domain.project.controller;

import com.example.moreveiw.domain.project.model.dto.response.ObjectResponse;
import com.example.moreveiw.domain.project.service.ProjectsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@Validated
@CrossOrigin("*")
@RequestMapping("/api")
@Tag(name = "Project Controller", description = "프로젝트 관련 API")
public class ProjectController {

    private final ProjectsService projectService;

    //친구 추가 요청받은 목록
    @Operation(summary = "Get Project Objects")
    @GetMapping("/project0/{projectId}")
    public ObjectResponse requestList(@PathVariable(value = "projectId") Long projectId) {
        return projectService.getProjectByObject(projectId);
    }

}