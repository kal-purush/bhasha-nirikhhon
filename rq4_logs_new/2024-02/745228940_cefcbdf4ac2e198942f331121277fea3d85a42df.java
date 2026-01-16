package org.example.JavaTaigaCode.controllers;

import java.util.List;

import org.example.JavaTaigaCode.models.UserStoryDTO;
import org.example.JavaTaigaCode.service.AdoptedWorkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin(origins = "http://localhost:3000")
@RequestMapping("/api")
public class AdoptedWorkController {
    @Autowired
    AdoptedWorkService adoptedWorkService;

    @GetMapping("/adoptedWork/{milestoneID}")
    @ResponseBody
    public List<UserStoryDTO> getUSAddedAfterSprintPlanning(@PathVariable("milestoneID") Integer milestoneID) {
        return adoptedWorkService.getUSAddedAfterSprintPlanning(milestoneID);
    }
}