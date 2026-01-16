package com.capstone.dentalclinic.demo.controller;

import javax.validation.Valid;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.capstone.dentalclinic.demo.model.UserModel;

@Controller
@RequestMapping("/system/login")
public class LoginController {
    
    // This will handle home page view of the page.
    @GetMapping("/admin")
    public String HomePageView() {
        return "admin/Login";
    } 
    
    @PostMapping("/admin")
    public UserModel login(@Valid @ModelAttribute UserModel userModel) {
        return null;
    }

}