package org.example.projectapp.controller;

import org.example.projectapp.controller.dto.ProjectMemberDto;
import org.example.projectapp.service.ProjectMemberService;
import org.example.projectapp.service.dto.ProjectMemberResponseDto;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@RestController
@RequestMapping("projects/{projectId}/members")
public class ProjectMembersController {
    private final ProjectMemberService memberService;

    public ProjectMembersController(ProjectMemberService memberService) {
        this.memberService = memberService;
    }

    @PostMapping
    public ResponseEntity<?> addOrUpdateProjectMember(@PathVariable Long projectId,
                                                      @RequestBody @Valid ProjectMemberDto dto) {
        ProjectMemberResponseDto responseDto = memberService.addMemberToProject(projectId, dto);
        return ResponseEntity.ok(responseDto);
    }
}