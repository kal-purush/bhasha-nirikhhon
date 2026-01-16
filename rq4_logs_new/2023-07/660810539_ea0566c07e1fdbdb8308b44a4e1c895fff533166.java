package com.practice.studentenrollment.username.controller;

import com.practice.studentenrollment.username.model.User;
import com.practice.studentenrollment.username.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v3/studentenrollment")
public class UserController {

    @Autowired
    private UserService userService;

    @PostMapping("/users")
    public ResponseEntity<User> addUser(@RequestBody User user) {

        if (userService.doesUserExist(user)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }


        User newUser = userService.addNewUser(user);


        return ResponseEntity.status(HttpStatus.CREATED).body(newUser);
    }

    @GetMapping("/users/{id}")
    public ResponseEntity<User> getUserById(@PathVariable("id") Long id) {

        User user = userService.getUserById(id);


        if (user == null) {
            return ResponseEntity.notFound().build();
        }


        return ResponseEntity.ok(user);
    }
}