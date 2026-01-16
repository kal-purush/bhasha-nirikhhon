package org.example.projectapp.controller;

import org.example.projectapp.controller.dto.SkillDto;
import org.example.projectapp.service.SkillsService;
import org.example.projectapp.service.dto.UserSkillDto;
import org.example.projectapp.service.exception.AlreadyAssignedSkillException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@RestController
@RequestMapping("skills")
public class SkillController {
    private final SkillsService skillsService;
    Logger logger = LoggerFactory.getLogger(SkillController.class);

    public SkillController(SkillsService skillsService) {
        this.skillsService = skillsService;
    }

    @PostMapping
    public ResponseEntity<UserSkillDto> addSkill(@RequestBody @Valid SkillDto skillDto) {
        UserSkillDto body = skillsService.addSkill(skillDto.getName(), skillDto.getExpertise());
        return ResponseEntity.ok(body);
    }

    @ExceptionHandler(AlreadyAssignedSkillException.class)
    public ResponseEntity<String> alreadyAssignedSkillHandling(AlreadyAssignedSkillException e) {
        logger.info("[SKILL] User {} already have such skill {}", e.getUserId(), e.getSkillId());
        //todo reconsider handling after frontend
        return ResponseEntity.badRequest().body("User already have such skill");
    }

}