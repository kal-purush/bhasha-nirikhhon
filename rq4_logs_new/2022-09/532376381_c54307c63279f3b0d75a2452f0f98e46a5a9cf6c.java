package com.example.sbemintic.controller;

import com.example.sbemintic.dtos.response.PermissionResponseDto;
import com.example.sbemintic.dtos.response.RoleResponseDto;
import com.example.sbemintic.services.RoleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("api/roles")
public class RoleController {
    @Autowired
    private RoleService Service;

    @GetMapping("/")
    public ResponseEntity<List<RoleResponseDto>> getAllRoles(){
        return ResponseEntity.ok(Service.getAllRoles());
    }

    @GetMapping("/{role:Admin|Jurado|Ciudadano|xx}")
    public ResponseEntity<List<PermissionResponseDto>> getAllPermissionsByRoleId(@PathVariable("role") String role){
        return  ResponseEntity.ok(Service.getAllPermission(role));
    }



}