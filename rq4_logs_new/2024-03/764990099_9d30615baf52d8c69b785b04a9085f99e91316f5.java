package com.customs.network.fdapn.controller;

import com.customs.network.fdapn.dto.AuthenticationRequest;
import com.customs.network.fdapn.model.AuthenticationResponse;
import com.customs.network.fdapn.model.RegisterRequest;
import com.customs.network.fdapn.service.AuthenticateService;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/auth")
@AllArgsConstructor
public class AuthenticateController {
    private final AuthenticateService authenticateService;
    @PostMapping("/register")
    public ResponseEntity<AuthenticationResponse> register(
            @RequestBody RegisterRequest request
    ) {
        return ResponseEntity.ok(authenticateService.register(request).getBody());
    }
    @PostMapping("/authenticate")
    public ResponseEntity<AuthenticationResponse> authenticate(
            @RequestBody AuthenticationRequest request
    ) {
        return ResponseEntity.ok(authenticateService.authenticate(request).getBody());
    }
}