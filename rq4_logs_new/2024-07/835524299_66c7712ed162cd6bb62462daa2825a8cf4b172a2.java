package com.company.empmgmtsystem.controller;

import com.company.empmgmtsystem.model.Department;
import com.company.empmgmtsystem.service.DepartmentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/departments")
public class DepartmentController {

    @Autowired
    private DepartmentService departmentService;

    @GetMapping
    public List<Department> getAllDepartments() {
        return departmentService.getAllDepartments();
    }


    @GetMapping("/{id}")
    public Department getDepartmentByID(@PathVariable long id) {
        return departmentService.getDepartmentById(id);
    }


    @PostMapping("/add")
    public Department createDepartment(@RequestBody Department department) {
        return departmentService.saveDepartment(department);
    }


    @PutMapping("/update/{id}")
    public Department updateDepartment(@PathVariable long id, @RequestBody Department department) {
        return departmentService.updateDepartment(id, department);
    }


    @DeleteMapping("/delete/{id}")
    public void deleteDepartment(@PathVariable long id) {
        departmentService.deleteDepartment(id);
    }

}