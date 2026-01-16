package com.exercise.usersregistration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/user")
public class UserController {
    @Autowired
    UserRepository userRepository;

//    @Autowired
//    public UserController(UserRepository userRepository) {
//        this.userRepository = userRepository;
//    }

    @PostMapping("/register")
    public String registerUser(@RequestBody UserModel user) {
        // Check if the username or emailId already exists
        if (userRepository.findByUsername(user.getUsername()) != null) {
            return "Username already exists!";
        }
        if (userRepository.findByEmailId(user.getEmailId()) != null) {
            return "Email already exists!";
        }

        // Set the default status to ACTIVE for a newly registered user
        user.setStatus(UserModel.Status.ACTIVE);

        // Save the user
        userRepository.save(user);
        return "Registration successful!";
    }

    @Getter
    @Setter
    @Component
    public static class LoginRequest {
        private String username;
        private String password;


        // Constructors, getters, setters (omitted for brevity)
    }

    @PostMapping("/login")
    public String loginUser(@RequestBody LoginRequest loginRequest) {
        UserModel user = userRepository.findByUsername(loginRequest.getUsername());
        if (user == null || !user.getPassword().equals(loginRequest.getPassword())) {
            return "Invalid username or password";
        }

        return "Login successful!";
    }
}