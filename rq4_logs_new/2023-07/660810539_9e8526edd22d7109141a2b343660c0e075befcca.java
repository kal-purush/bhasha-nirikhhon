package com.practice.studentenrollment.username.controller;

import com.practice.studentenrollment.username.service.UserService;
import com.practice.studentenrollment.username.model.UserName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v3/studentenrollment")
public class UserController {

    @Autowired
    private UserService userService;

    @GetMapping("/users/availability")
    public ResponseEntity<String> checkUsernameAvailability(@RequestParam("username") String username) {
        // Check if the username already exists
        boolean isUsernameExists = userService.isUsernameExists(username);

        if (isUsernameExists) {
            return ResponseEntity.status(HttpStatus.OK).body("Username already exists");
        } else {
            // Store the username
            UserName newUser = userService.storeUserName(username);
            return ResponseEntity.ok("Username '" + newUser.getUsername() + " is available and stored");
        }
    }
}