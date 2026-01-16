package com.geekbrains.controller;

import com.geekbrains.model.Role;
import com.geekbrains.model.User;
import com.geekbrains.service.RoleService;
import com.geekbrains.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.annotation.Secured;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Controller
public class GeneralControllerTH {


    @GetMapping("/login")
    public String getLoginPage() {
        return "login";
    }


}