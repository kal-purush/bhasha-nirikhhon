package com.filegenie.backend.Controllers;

import com.filegenie.backend.DTO.GenericResponse;
import com.filegenie.backend.DTO.LoginRequest;
import com.filegenie.backend.DTO.LoginResponse;
import com.filegenie.backend.DTO.RegisterRequest;
import com.filegenie.backend.Services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/auth")
public class AuthController {
    @Autowired
    private UserService userService;

    @PostMapping("/register")
    public ResponseEntity<GenericResponse> registerUser(@RequestBody RegisterRequest req) {
        userService.registerUser(req);
        GenericResponse response = new GenericResponse();
        response.setMessage("User registered");

        return ResponseEntity.ok(response);
    }

    @PostMapping("/login")
    public ResponseEntity<LoginResponse> loginUser(@RequestBody LoginRequest req) {
        String sessionToken = userService.loginUser(req);
        LoginResponse response = new LoginResponse();
        response.setToken(sessionToken);

        return ResponseEntity.ok(response);
    }

}