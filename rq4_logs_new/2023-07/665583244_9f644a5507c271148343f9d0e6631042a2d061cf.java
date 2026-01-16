package project.vilsoncake.BookOnlineStore.controller;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import project.vilsoncake.BookOnlineStore.service.AdminService;

@Controller
@RequestMapping("/admin")
public class AdminController {

    private final AdminService adminService;

    public AdminController(AdminService adminService) {
        this.adminService = adminService;
    }

    @PreAuthorize("hasAuthority('ADMIN')")
    @GetMapping("/get-user")
    public String getUserPage(Model model) {
        return "user-data.html";
    }

    @PreAuthorize("hasAuthority('ADMIN')")
    @PostMapping("/get-user")
    public String findUser(@RequestParam String userId, Model model) {
        return adminService.findUser(userId, model);
    }
}