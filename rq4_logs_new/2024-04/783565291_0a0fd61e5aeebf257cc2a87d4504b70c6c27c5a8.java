package com.engineCore.READSERVICE.controller;

import com.engineCore.READSERVICE.entity.UserEntity;
import com.engineCore.READSERVICE.exception.UserNotFoundExceptionHandler;
import com.engineCore.READSERVICE.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
public class UserController {

    @Autowired
    private UserService userService;

    @GetMapping("/users")
    public ResponseEntity<List<UserEntity>> getAllUsers() {
        List<UserEntity> users = userService.getAll();
        return new ResponseEntity<>(users, HttpStatus.OK);
    }

    @GetMapping("/user/{id}")
    public ResponseEntity<UserEntity> getuserById(@PathVariable Long id) throws UserNotFoundExceptionHandler {
        UserEntity user = userService.getById(id);
        return new ResponseEntity<>(user, HttpStatus.OK);
    }
}