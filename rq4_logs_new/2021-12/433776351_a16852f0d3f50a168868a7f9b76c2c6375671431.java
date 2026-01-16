package com.AtelierGit.AtelierGit.controller;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin
public class Index {


    @GetMapping("/")
    public String index (){

        return "<h1>Hello world from Tomcat TP Ansible</h2>";
    }
}