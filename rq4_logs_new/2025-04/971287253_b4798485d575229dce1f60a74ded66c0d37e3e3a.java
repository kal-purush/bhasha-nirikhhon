package com.example.socio.controller;

import com.example.socio.model.AuthenticationResponse;
import com.example.socio.model.LoginRequest;
import com.example.socio.model.RegisterRequest;
import com.example.socio.model.ResetPasswordRequest;
import com.example.socio.service.AuthenticationService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/auth")
public class AuthenticationController {

    @Autowired
    private AuthenticationService authService;

    @PostMapping("/register")
    public ResponseEntity<String> registerUser(@Valid @RequestBody RegisterRequest registerRequest) {
        authService.registerUser(registerRequest);
        return ResponseEntity.ok("User registered successfully");
    }

   @PostMapping("/login")
   public ResponseEntity<AuthenticationResponse> loginUser(@Valid @RequestBody LoginRequest loginRequest) {
       AuthenticationResponse authResponse = authService.authenticateUser(loginRequest);
       return ResponseEntity.ok(authResponse);
   }

    @PostMapping("/reset-password")
    public ResponseEntity<String> resetPassword(@Valid @RequestBody ResetPasswordRequest resetPasswordRequest) {
        authService.resetPassword(resetPasswordRequest);
        return ResponseEntity.ok("Password reset successfully");
    }
}