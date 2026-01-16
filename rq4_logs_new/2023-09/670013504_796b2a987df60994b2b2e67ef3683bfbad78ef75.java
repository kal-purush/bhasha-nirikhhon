package com.grievance.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.grievance.dto.DepartmentDto;
import com.grievance.entity.Department;
import com.grievance.service.DepartmentService;

@RestController
@CrossOrigin("*")
@RequestMapping("/department")
public class DepartmentController {
	
	@Autowired
	DepartmentService departmentService;
	
	@PostMapping("/save")
	public ResponseEntity<?> saveDepartment(@RequestBody DepartmentDto departmentDto){
//		return new ResponseEntity<>(departmentService.saveDepartment(departmentDto), HttpStatus.OK);
		return ResponseEntity.ok(departmentService.saveDepartment(departmentDto));
	}
	
	@GetMapping("/listDepartments")
	public ResponseEntity<?> listDepartments(){
//		return ResponseEntity.ok(departmentService.listAllDepartments());
		return ResponseEntity.ok(departmentService.listAllDepartment());
	}
}
