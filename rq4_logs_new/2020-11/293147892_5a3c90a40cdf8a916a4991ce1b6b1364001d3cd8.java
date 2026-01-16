package com.pnu.dev.shpaltaif.controller;

import com.pnu.dev.shpaltaif.service.LoginAttemptService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping("/admin/security-info")
public class LoginAttemptController {

    private final LoginAttemptService loginAttemptService;

    @Autowired
    public LoginAttemptController(LoginAttemptService loginAttemptService) {
        this.loginAttemptService = loginAttemptService;
    }

    @GetMapping
    public String findAll(@PageableDefault(size = 15, sort = "dateTime", direction = Sort.Direction.DESC)
                                  Pageable pageable,
                          @RequestParam(required = false, name = "blockedOnly", defaultValue = "false")
                                  boolean blockedOnly,
                          Model model) {
        model.addAttribute("failedLoginAttemptsInfo", loginAttemptService.getFailedLoginAttemptsInfo());
        model.addAttribute("loginAttempts", loginAttemptService.findAll(pageable, blockedOnly));
        model.addAttribute("blockedOnly", blockedOnly);
        return "admin/loginAttempts/index";
    }
}