package org.example.projectapp.controller;

import org.example.projectapp.controller.dto.VacancyDto;
import org.example.projectapp.model.Vacancy;
import org.example.projectapp.service.VacancyService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@RequestMapping("vacancies")
public class VacancyController {
    private final VacancyService vacancyService;

    public VacancyController(VacancyService vacancyService) {
        this.vacancyService = vacancyService;
    }

    @PostMapping
    public ResponseEntity<Vacancy> createVacancy(@RequestBody @Valid VacancyDto dto) {
        Vacancy vacancy = vacancyService.createVacancy(dto);
        return ResponseEntity.ok(vacancy);
    }
}