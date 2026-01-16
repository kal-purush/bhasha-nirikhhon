package com.reddot.app.controller;

import com.reddot.app.dto.response.NotificationDTO;
import com.reddot.app.dto.response.ServiceResponse;
import com.reddot.app.entity.User;
import com.reddot.app.exception.ResourceNotFoundException;
import com.reddot.app.service.notification.NotificationService;
import com.reddot.app.service.system.SystemAuthentication;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/notifications")
public class NotificationController {

    private final NotificationService notificationService;

    public NotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    /**
     * Get all notifications for the logged-in user.
     *
     * @return ResponseEntity containing the status code, message, and list of notifications
     */
    @GetMapping()
    public ResponseEntity<ServiceResponse<List<NotificationDTO>>> getAllNotifications() {
        try {
            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

            if (SystemAuthentication.isLoggedIn(authentication)) {
                User user = (User) authentication.getPrincipal();
                List<NotificationDTO> notifications = notificationService.getAllUnreadNotificationByUser(user);

                notificationService.markNotificationsAsRead(user);

                if(notifications != null) {

                }

                return ResponseEntity.ok(new ServiceResponse<>(200, "Notifications retrieved successfully", notifications));
            } else {
                return ResponseEntity.status(401).body(new ServiceResponse<>(401, "User not authenticated", null));
            }
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body(new ServiceResponse<>(500, "Internal server error", null));
        }
    }

}