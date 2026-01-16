package com.koscom.demo.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.koscom.demo.protocol.domain.User;

@Controller
@RequestMapping("/main")
public class MainController {
    
    @GetMapping
    public String mainController(Model model) {
        
        // 1. Object
        User user = new User();
        user.setUserName("홍길동");
        user.setUserType("기업");

        // 2. List
        List<User> users = new ArrayList<>();
        users.add(user);

        // 3. Map
        Map<String, User> userMap = new HashMap<>();
        userMap.put("userMap", user);

        model.addAttribute("user", user);
        model.addAttribute("users", users);
        model.addAttribute("userMap", userMap);
        
        return "basic/main";
    }
    
}