package com.practice.studentenrollment.familyinfo.controller;

import com.practice.studentenrollment.familyinfo.model.Parents;
import com.practice.studentenrollment.familyinfo.model.Siblings;
import com.practice.studentenrollment.familyinfo.service.ParentsService;
import com.practice.studentenrollment.familyinfo.service.SiblingsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("api/v1/family")
public class FamilyController {

    @Autowired
    private ParentsService parentsService;

    @Autowired
    private SiblingsService siblingsService;


    @PostMapping("/parents")
    public Parents addNew(@RequestBody Parents parents) {
        return this.parentsService.addNew(parents);
    }

    @GetMapping("/parents/all")
    public List<Parents> getAllDetails() {
        return this.parentsService.getAllDetails();
    }

    @PostMapping("/siblings")
    public Siblings addNew(@RequestBody Siblings siblings) {
        return this.siblingsService.addNew(siblings);
    }

    @GetMapping("/siblings/all")
    public List<Siblings> getAllSiblings() {
        return this.siblingsService.getAllSiblings();
    }
}