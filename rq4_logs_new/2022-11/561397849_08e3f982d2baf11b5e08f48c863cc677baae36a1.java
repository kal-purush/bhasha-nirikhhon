package com.mvc.educentr.api.controllers;

import com.mvc.educentr.models.User;
import com.mvc.educentr.api.service.FirebaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;

import java.util.concurrent.ExecutionException;

@org.springframework.web.bind.annotation.RestController
public class RestController {

    @Autowired
    FirebaseService firebaseService;

    @GetMapping("/getUserDetails")
    public User getExample(@RequestHeader() String name) throws InterruptedException, ExecutionException {
        return new User(name, "30", "Dallas");
    }



    @PostMapping("/createUser")
    public String postExample(@RequestBody User person) throws InterruptedException, ExecutionException {
        firebaseService.saveUserDetails(person);
        return "Created User " + person.getInitials();
    }
}