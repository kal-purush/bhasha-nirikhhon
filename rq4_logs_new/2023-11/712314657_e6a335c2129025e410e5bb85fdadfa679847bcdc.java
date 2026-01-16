package az.ingress.bookstore.controller;

import az.ingress.bookstore.dto.request.ChangePasswordRequest;
import az.ingress.bookstore.dto.request.StudentRequest;
import az.ingress.bookstore.dto.response.StudentResponse;
import az.ingress.bookstore.service.StudentService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/student")
public class StudentController {
    private final StudentService studentService;

    public StudentController(StudentService studentService) {
        this.studentService = studentService;
    }

    @PostMapping("/changePassword")
    public ResponseEntity<String> changePassword(@RequestBody ChangePasswordRequest request) {
        studentService.changePassword(request);
        return ResponseEntity.status(HttpStatus.OK).build();
    }

    @PutMapping("/update")
    public ResponseEntity<StudentResponse> updateMyAccount(StudentRequest request) {
        return studentService.updateMyAccount(request);
    }

    @DeleteMapping("/delete")
    public ResponseEntity<String> deleteMyAccount() {
        studentService.deleteMyAccount();
       return ResponseEntity.status(HttpStatus.OK).build();
    }
}