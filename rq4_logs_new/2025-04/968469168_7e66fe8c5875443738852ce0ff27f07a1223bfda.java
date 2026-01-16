package com.doubleo.memberservice.domain.employee.controller;

import com.doubleo.memberservice.domain.employee.domain.Employee;
import com.doubleo.memberservice.domain.employee.dto.request.EmployeeCreateRequest;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Tag(name = "1-1. Employee API", description = "직원 API")
@RestController
@RequiredArgsConstructor
@RequestMapping("/employee")
public class EmployeeController {

    // 직원 생성
    @PostMapping("/create")
    public ResponseEntity<Employee> employeeCreate(@RequestBody EmployeeCreateRequest request) {
        return ResponseEntity.ok(new Employee());
    }

    // 직원 개별 정보 조회
    @GetMapping("/{employeeId}")
    public ResponseEntity<Employee> employeeGet() {
        return ResponseEntity.ok(new Employee());
    }

    // 직원 전체 목록 조회
    @GetMapping("/")
    public ResponseEntity<Employee> employeeListGet() {
        return ResponseEntity.ok(new Employee());
    }

    // 직원 정보 업데이트
    @PatchMapping("/{employeeId}")
    public ResponseEntity<Employee> employeeUpdate() {
        return ResponseEntity.ok(new Employee());
    }

    // 직원 프로필 이미지 업데이트
    @PatchMapping("/{employeeId}/profile-image")
    public ResponseEntity<Employee> profileImageUpdate() {
        return ResponseEntity.ok(new Employee());
    }

    // 직원 삭제
    @DeleteMapping("/{employeeId}")
    public ResponseEntity<Employee> employeeDelete() {
        return ResponseEntity.ok(new Employee());
    }
}