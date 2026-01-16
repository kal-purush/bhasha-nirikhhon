package com.reddot.app.scheduler;

import com.reddot.app.service.user.StatisticsService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
@Component
public class BadgeScheduler {
    private final StatisticsService statisticsService;

    public BadgeScheduler(StatisticsService statisticsService) {
        this.statisticsService = statisticsService;
    }

    // Định nghĩa tác vụ chạy mỗi phút
    @Scheduled(fixedRate = 60000) // Mỗi 60 giây
    public void assignBadgesAutomatically() {
        try {
            statisticsService.assignBadgesToAllUsers(); // Logic gán badge
            System.out.println("Badges assigned successfully at " + LocalDateTime.now());
        } catch (Exception e) {
            System.err.println("Error assigning badges: " + e.getMessage());
        }
    }
}