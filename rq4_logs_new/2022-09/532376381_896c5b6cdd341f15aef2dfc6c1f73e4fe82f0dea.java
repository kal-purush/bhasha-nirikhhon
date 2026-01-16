package com.example.sbemintic.controller;

import com.example.sbemintic.dtos.response.UserResponseDto;
import com.example.sbemintic.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController //para que sea considerado un controlador
@RequestMapping("api/users") //sobre la ruta a la que responderá
public class UsersController {

    @Autowired
    private UserService service;

    @GetMapping("/")
    public ResponseEntity <List<UserResponseDto>> getAllUsers(){
        return ResponseEntity.ok(service.getAllUsers());
    }


}