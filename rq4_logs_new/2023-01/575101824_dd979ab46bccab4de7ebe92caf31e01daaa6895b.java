package az.spring.studentmanagementservice.studentmanagementservice.controller;

import az.spring.studentmanagementservice.studentmanagementservice.request.StudentRequest;
import az.spring.studentmanagementservice.studentmanagementservice.response.StudentListResponse;
import az.spring.studentmanagementservice.studentmanagementservice.response.StudentResponse;
import az.spring.studentmanagementservice.studentmanagementservice.service.StudentService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@RequestMapping("/students")
@RequiredArgsConstructor
public class StudentController {

    private final StudentService studentService;

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public StudentListResponse getAll() {
        return studentService.getAllStudents();
    }

    @GetMapping("/{studentId}")
    @ResponseStatus(HttpStatus.OK)
    public StudentResponse getStudent(@PathVariable Long studentId) {
        return studentService.getStudentById(studentId);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public StudentResponse createStudent(@Valid @RequestBody StudentRequest studentRequest) {
       return studentService.createStudent(studentRequest);
    }

    @PutMapping("/{studentId}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public StudentResponse updateStudent(@PathVariable Long studentId, @Valid @RequestBody StudentRequest studentRequest) {
        return studentService.updateStudent(studentId, studentRequest);
    }

    @DeleteMapping("/{studentId}")
    @ResponseStatus(HttpStatus.OK)
    public void deleteStudent(@PathVariable Long studentId) {
        studentService.deleteStudent(studentId);
    }

}