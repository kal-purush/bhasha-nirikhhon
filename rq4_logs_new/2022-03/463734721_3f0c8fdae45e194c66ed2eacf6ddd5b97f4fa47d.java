package com.springmvc.controller;

import com.springmvc.entity.User;


import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.stereotype.Controller;

/**
 * @author: lijinhao
 * @date: 2022/03/01 17:43
 */

@Controller
//@RequestMapping("/test")
public class URLMappingController {
    @GetMapping("/g")
    @ResponseBody
    public String getMapping(@RequestParam("manager_name") String managerName){
        System.out.println("manager:" + managerName);
        return "This is a get method";
    }

    @PostMapping("/p")
    @ResponseBody
    public String postMapping(String username, Long password){
        System.out.println("username:" + username + "——password:" + password);
        return "This is a post method";
    }

    @PostMapping("/p1")
    @ResponseBody
    public String postMapping1(User user){
        System.out.println(user.getUsername() + ":" + user.getPassword());
        return "This is post method";
    }
}