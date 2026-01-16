package com.practice.studentenrollment.SearchApi.Controller;

import com.practice.studentenrollment.SearchApi.Model.StudentSearch;
import com.practice.studentenrollment.SearchApi.Service.StudentSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1/students")
public class StudentSearchController {
    @Autowired
    private StudentSearchService studentSearchService;

    @PostMapping("/add")
    public StudentSearch addStudents(@RequestBody StudentSearch studentSearch) {
        return studentSearchService.addStudents(studentSearch);
    }

    @GetMapping("/all")
    public List<StudentSearch> getStudentSearch() {
        return studentSearchService.studentSearchList();
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getStudentByID(@PathVariable Integer id) {
        Optional<StudentSearch> student = studentSearchService.getStudentByid(id);

        if (student.isEmpty()) {
            // Student not found with the given ID
            String errorMessage = "Invalid: No student found with the given ID. Please try again.";
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorMessage);
        }

        // Student found, return the student object
        return ResponseEntity.ok(student);
    }

    @GetMapping("/search")
    public ResponseEntity<?> getStudentByFirstName(@RequestParam(value = "firstName", required = false) String firstName) {
        if (firstName == null || firstName.isEmpty()) {
            // First name not provided
            String errorMessage = "Invalid: First name is required. Please try again.";
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorMessage);
        }

        List<StudentSearch> students = studentSearchService.getStudentsByFirstName(firstName);

        if (students.isEmpty()) {
            // First name not found in the table
            String errorMessage = "Invalid: No student found with the given first name. Please try again.";
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorMessage);
        }

        // First name found, return the list of students
        return ResponseEntity.ok(students);
    }

    @GetMapping("/search/lastName")
    public ResponseEntity<?> getStudentByLastName(@RequestParam("lastName") String lastName) {
        List<StudentSearch> students = studentSearchService.getStudent(lastName);

        if (students.isEmpty()) {
            // Last name not found in the table
            String errorMessage = "Invalid: No student found with the given last name. Please try again.";
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorMessage);
        }

        // Last name found, return the list of students
        return ResponseEntity.ok(students);
    }

    @GetMapping("/search/username")
    public ResponseEntity<?> getStudentsByUsername(@RequestParam("userName") String userName) {
        List<StudentSearch> students = studentSearchService.studentByUserName(userName);

        if (students.isEmpty()) {
            // Username not found in the table
            String errorMessage = "Invalid: No student found with the given username. Please try again.";
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorMessage);
        }

        // Username found, return the list of students
        return ResponseEntity.ok(students);
    }

    @GetMapping("/search/name")
    public ResponseEntity<?> getStudentsByName(@RequestParam("name") String name) {
        List<StudentSearch> students = studentSearchService.studentByname(name);

        if (students.isEmpty()) {
            // Name not found in the table
            String errorMessage = "Invalid: No student found with the given name. Please try again.";
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorMessage);
        }

        // Name found, return the list of students
        return ResponseEntity.ok(students);
    }
}