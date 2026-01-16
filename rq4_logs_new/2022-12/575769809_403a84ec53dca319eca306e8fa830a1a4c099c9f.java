package com.project.attendancemanagementsystem.controller;

import com.project.attendancemanagementsystem.model.StudentRequest;
import com.project.attendancemanagementsystem.model.StudentResponse;
import com.project.attendancemanagementsystem.service.AdminService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class AdminController {

    private final AdminService adminService;

    public AdminController(AdminService adminService) {
        this.adminService = adminService;
    }

    @PostMapping("/admin/students")
    public void addStudent(@RequestBody StudentRequest studentRequest) {
        adminService.addStudent(studentRequest);
    }

    @GetMapping("/admin/students")
    public List<StudentResponse> getStudents() {
        return adminService.getStudents();
    }

    @PutMapping("/admin/students")
    public void updateStudent(@RequestBody StudentRequest request) {
        adminService.updateStudent(request);
    }

    @DeleteMapping("/admin/students")
    public void deleteStudent(@RequestParam List<Long> id) {
        adminService.deleteStudent(id);
    }

}