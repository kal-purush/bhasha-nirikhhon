package com.example.sbemintic.controller;

import com.example.sbemintic.dtos.request.LoginRequestDto;
import com.example.sbemintic.dtos.response.UserResponseDto;
import com.example.sbemintic.services.AuthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@RequestMapping("api/auth")
public class AuthController {
    @Autowired
    private AuthService service;

    @PostMapping("/")
    public ResponseEntity<UserResponseDto> login(@RequestBody @Valid LoginRequestDto request){
        return ResponseEntity.ok(this.service.login(request.getEmail(),request.getPassword()));
    }
}