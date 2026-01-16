package com.company.getyourgoal.controller;

import com.company.getyourgoal.model.User;
import com.company.getyourgoal.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
public class UserController {

    @Autowired
    private UserRepository userRepository;

    @PostMapping("/users")
    @ResponseStatus(value = HttpStatus.CREATED)
    public User createUser(@RequestBody User user) {

        return userRepository.save(user);
    }

    @GetMapping("/users")
    @ResponseStatus(value = HttpStatus.OK)
    public List<User> getUserList() {
        return userRepository.findAll();
    }

    @GetMapping("/users/{id}")
    @ResponseStatus(value = HttpStatus.OK)
    public User getUserById(@PathVariable("id") int id) {
        Optional<User> returnVal = userRepository.findById(id);
        if (returnVal.isPresent()) {
            return returnVal.get();
        } else {
            return null;
        }
    }

    @PutMapping("/users/{id}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public User updateUser(@RequestBody   User user,
                           @PathVariable("id") int id) {
        return userRepository.save(user);

    }
    @DeleteMapping("/users/{id}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public String  deleteUserById(@PathVariable("id") int id) {
        userRepository.deleteById(id);
        return "Post deleted Successfully !!!!!";
    }
}