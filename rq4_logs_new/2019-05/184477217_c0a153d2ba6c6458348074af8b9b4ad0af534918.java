package com.triviamachine.triviamachine.controllers;

import com.triviamachine.triviamachine.database.*;
import org.apache.catalina.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.view.RedirectView;

@Controller
@RequestMapping
public class ApplicaitonController {

    @Autowired
    private QuestionRepository questionRepo;
    @Autowired
    private QuestionScheduleRepository questionScheduleRepo;
    @Autowired
    private ResultsRepository resultsRepo;
    @Autowired
    AdminUserRepository adminRepo;

    @GetMapping("/login")
    public String getLogin(@AuthenticationPrincipal AdminUser user, Model model){
        model.addAttribute("user", user);
        return "login";
    }

}