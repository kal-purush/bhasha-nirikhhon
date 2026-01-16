package com.bridgelabz.SmartContactManager.controllers;

import com.bridgelabz.SmartContactManager.dao.UserRepository;
import com.bridgelabz.SmartContactManager.entities.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class HomeController {
    @Autowired
    private UserRepository userRepository;

    @GetMapping("/test")
    @ResponseBody
    public String test(){
        User user1 = new User();
        user1.setName("Abhishek Kumar");
        user1.setEmail("abhishek.ice15@gmail.com");
        userRepository.save(user1);
        return "working";
    }
}