package com.springmvc.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

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

}