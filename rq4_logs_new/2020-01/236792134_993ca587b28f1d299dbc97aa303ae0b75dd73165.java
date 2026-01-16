package com.example.demo3.controller;

import com.example.demo3.entity.User;
import com.example.demo3.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/user")
public class Demo3Controller {

    @Autowired
    UserService userService;
    @GetMapping("/getUser")
    public User getDemo3(@RequestParam int userId){
        User user = null;
        try{
            user =  userService.gerUserById(userId);
        }catch(Exception Ex){

        }
        return user;
    }


}