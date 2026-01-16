package com.example.MusicApp.controller;

import com.example.MusicApp.service.ResetPasswordService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@CrossOrigin(origins = "*") // For testing only
@RestController
@RequestMapping("/api/auth")
public class ResetPasswordController {

    @Autowired
    private ResetPasswordService resetPasswordService;

    @PostMapping("/reset-password")
    public ResponseEntity<String> resetPassword(@RequestBody Map<String, String> request) {
        return resetPasswordService.resetPassword(request.get("email"), request.get("password"));
    }
}