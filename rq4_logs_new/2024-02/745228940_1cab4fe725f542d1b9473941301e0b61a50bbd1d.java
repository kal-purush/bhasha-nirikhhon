package org.example.JavaTaigaCode.controllers;

import org.example.JavaTaigaCode.models.TaskDTO;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@CrossOrigin(origins = "http://localhost:3000") // Allow access from frontend server (React)
@RequestMapping("/api")
public class FoundWorkController {
    @GetMapping("/foundWork/{milestoneID}")
    @ResponseBody
    public List<TaskDTO> getFoundWorkByID(@PathVariable("milestoneID") Integer milestoneID) {
        return null;
//        return projectService.getClosedMilestonesbyID(projectID);
    }

}