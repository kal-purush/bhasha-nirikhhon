package com.pnu.dev.shpaltaif.controller;

import com.pnu.dev.shpaltaif.domain.User;
import com.pnu.dev.shpaltaif.domain.UserRole;
import com.pnu.dev.shpaltaif.dto.telegram.TelegramSubscriptionsDashboardInfo;
import com.pnu.dev.shpaltaif.service.telegram.TelegramBotUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/admin")
public class IndexAdminController {

    private TelegramBotUserService telegramBotUserService;

    @Autowired
    public IndexAdminController(TelegramBotUserService telegramBotUserService) {
        this.telegramBotUserService = telegramBotUserService;
    }

    @GetMapping
    public String index(@AuthenticationPrincipal User user, Model model) {

        if (user.getRole() == UserRole.ROLE_ADMIN) {
            TelegramSubscriptionsDashboardInfo dashboardInfo = telegramBotUserService.getSubscriptionsDashboardInfo();
            model.addAttribute("telegramDashboard", dashboardInfo);
        }

        return "admin/index";
    }

}