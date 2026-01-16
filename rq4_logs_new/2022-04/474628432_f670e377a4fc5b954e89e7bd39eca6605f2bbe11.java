package com.example.registerstartupproject.adminPanel;

import com.example.registerstartupproject.Repository.Entity.RegisterTeam;
import com.example.registerstartupproject.Repository.Entity.Status;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@CrossOrigin(origins = "${frontEndLink}", allowedHeaders = "*")
@RequiredArgsConstructor
public class AdminController {

    private final AdminService adminService;

@GetMapping("/admin/getTeams")
public List<RegisterTeam> getAllTeams()
{

    return adminService.registerTeams();
}
@PostMapping("/admin/setQualifcationStatusAndComment/{id}")
public void setQualificationStatus(@PathVariable Long id, @RequestBody IdeaFromAdminDTO status)
{
adminService.setQualificationStatus(id,status);


}


}