package com.brainstrom.meokjang.admin.auth.controller;

import com.brainstrom.meokjang.admin.auth.dto.AdminLoginForm;
import com.brainstrom.meokjang.admin.auth.service.AdminAuthService;
import jakarta.servlet.http.HttpSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

import java.util.List;

@Controller
public class AdminAuthController {

    private final AdminAuthService adminAuthService;

    @Autowired
    public AdminAuthController(AdminAuthService adminAuthService) {
        this.adminAuthService = adminAuthService;
    }

    @GetMapping("/admin")
    public String adminLogin(Model model, HttpSession httpSession) {
        Boolean canAccessAdminPage = (Boolean) httpSession.getAttribute("canAccessAdminPage");
        if (canAccessAdminPage != null && canAccessAdminPage) {
            List<Integer> dealCountList = adminAuthService.getDealCountList();
            model.addAttribute("list", dealCountList);
            return "adminMain";
        } else {
            return "adminLogin";
        }
    }

    @PostMapping("/admin")
    public String adminLoginPost(AdminLoginForm adminLoginForm, Model model, HttpSession httpSession) {
        try {
            if (adminAuthService.adminLogin(adminLoginForm)) {
                httpSession.setAttribute("canAccessAdminPage", true);
                List<Integer> dealCountList = adminAuthService.getDealCountList();
                model.addAttribute("list", dealCountList);
                return "adminMain";
            } else {
                return "adminLogin";
            }
        }
        catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return "adminLogin";
        }
    }
}