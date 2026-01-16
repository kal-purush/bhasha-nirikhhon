package com.api.NomNomCounter.adaptors.in.controller.user;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.api.NomNomCounter.adaptors.in.persistence.user.UserEntity;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


@RestController
@RequestMapping("users")
public class UserController {
    @PostMapping()
    public UserEntity user(@RequestBody UserEntity entity) {        
        return entity;
    }
    
}