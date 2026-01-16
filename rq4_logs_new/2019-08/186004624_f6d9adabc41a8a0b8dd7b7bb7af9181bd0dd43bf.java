package com.test.springboot.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.test.springboot.entity.User;

@Controller
@RequestMapping("/jstl/")
public class JSTLController {

    
    @RequestMapping("getUser")
    public String getUser(HttpServletRequest request){
        User user = new User(1, "张三");
        request.setAttribute("user", user);
        return "jstl/index-2";
    }
    
    @RequestMapping("setUser")
    public String setUser(User user){
        System.out.println(user);
        return "jstl/index-2";
    }
    
    @RequestMapping("userList")
    public String getUsers(HttpServletRequest request){
        List<User> list = new ArrayList<User>();
        User user = new User(1,"张三");
        User user2 = new User(2,"李四");
        list.add(user);
        list.add(user2);
        request.setAttribute("list", list);
        return "jstl/index-2";
    }
    
}