package com.project.attendancemanagementsystem.controller;

import com.project.attendancemanagementsystem.model.Student;
import com.project.attendancemanagementsystem.model.StudentResponse;
import com.project.attendancemanagementsystem.service.StudentRequest;
import com.project.attendancemanagementsystem.service.StudentService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
public class StudentController {

    private final StudentService studentService;

    public StudentController(StudentService studentService) {
        this.studentService = studentService;
    }

    @GetMapping("/students")
    public List<StudentResponse> getStudents() {
        return studentService.getStudents();
    }

    @PostMapping("/students")
    public void addStudent(@RequestBody StudentRequest request) {
        studentService.addStudent(request);
    }


}