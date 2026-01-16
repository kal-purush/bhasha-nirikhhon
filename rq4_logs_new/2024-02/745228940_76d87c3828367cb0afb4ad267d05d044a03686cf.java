package org.example.JavaTaigaCode.controllers;

import java.util.List;

import org.example.JavaTaigaCode.models.MilestoneDTO;
import org.example.JavaTaigaCode.service.ProjectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin(origins = "http://localhost:3000") // Allow access from frontend server (React)
@RequestMapping("/api")
public class DoTController {

    @Autowired
    ProjectService projectService;

    @GetMapping("/DoT/{projectID}")
    @ResponseBody
    public List<MilestoneDTO> getClosedMilestonesbyID(@PathVariable("projectID") Integer projectID) {
        return projectService.getClosedMilestonesbyID(projectID);
    }

    @GetMapping("/DoT/by-slug/{Slug}")
    @ResponseBody
    public List<MilestoneDTO> getClosedMilestonesbySlug(@PathVariable("Slug") String Slug) {
        return projectService.getClosedMilestonesbySlug(Slug);
    }   
}