package com.securitydemo.controller;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
class DemoController {
    @GetMapping("/user-endpoint")
    @PreAuthorize("hasRole('USER')")
    public String userAccess() {
        return "Hello User! You have USER role access.";
    }

    @GetMapping("/admin-endpoint")
    @PreAuthorize("hasRole('ADMIN')")
    public String adminAccess() {
        return "Hello Admin! You have ADMIN role access.";
    }

    @GetMapping("/common-endpoint")
    @PreAuthorize("hasAnyRole('USER', 'ADMIN')")
    public String commonAccess() {
        return "Hello! Both USER and ADMIN roles can access this endpoint.";
    }
}