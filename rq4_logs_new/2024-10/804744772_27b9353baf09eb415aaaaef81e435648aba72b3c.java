package com.example.controller;

import com.example.model.Admin;
import com.example.service.AdminService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/admin")
public class AdminController {
    private final AdminService adminService;

    public AdminController(AdminService adminService) {
        this.adminService = adminService;
    }

    @PostMapping("/create")
    public ResponseEntity<Admin> createAdmin(@RequestBody Admin admin) {
        Admin createdAdmin = adminService.createAdmin(admin);
        return ResponseEntity.ok(createdAdmin);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Admin> getAdminById(@PathVariable Long id) {
        Admin admin = adminService.getAdminById(id);
        return ResponseEntity.ok(admin);
    }

    @PostMapping("/deactivate/{id}")
    public ResponseEntity<Admin> deactivateAdmin(@PathVariable Long id) {
        Admin admin = adminService.deactivateAdmin(id);
        return ResponseEntity.ok(admin);
    }

    @PostMapping("/assign-role/{id}")
    public ResponseEntity<Admin> assignRole(@PathVariable Long id, @RequestBody String role) {
        Admin admin = adminService.assignRole(id, role);
        return ResponseEntity.ok(admin);
    }

    @PostMapping("/make-user-admin/{id}")
    public ResponseEntity<Admin> makeUserToAdmin(@PathVariable Long id) {
        Admin newAdmin = adminService.makeUserToAdmin(id);
        return ResponseEntity.ok(newAdmin);
    }

    @PutMapping("/update-profile/{id}")
    public ResponseEntity<Admin> changeAdminProfile(@PathVariable Long id, @RequestBody Admin admin) {
        Admin updatedAdmin = adminService.changeAdminProfile(id, admin);
        return ResponseEntity.ok(updatedAdmin);
    }

    @PutMapping("/change-password/{id}")
    public ResponseEntity<Admin> changeAdminPassword(@PathVariable Long id, @RequestBody String password) {
        Admin updatedAdmin = adminService.changeAdminPassword(id, password);
        return ResponseEntity.ok(updatedAdmin);
    }
}