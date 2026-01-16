package org.detective.controller.Notify;

import org.detective.dto.NotificationDTO;
import org.detective.services.Notify.NotificationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping("/notification")
public class NotificationController {

    private final NotificationService notificationService;
    public static Map<Long,SseEmitter> sseEmitters = new ConcurrentHashMap<>();

    public NotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @GetMapping("/subscribe")
    public SseEmitter subscribe(@RequestParam Long userId) {
        System.err.println("제발 여기까지라도ㅠㅠ 제발 여기까지라도ㅠㅠ 제발 여기까지라도ㅠㅠ 제발 여기까지라도ㅠㅠ 제발 여기까지라도ㅠㅠ 제발 여기까지라도ㅠㅠ 제발 여기까지라도ㅠㅠ 제발 여기까지라도ㅠㅠ 제발 여기까지라도ㅠㅠ 제발 여기까지라도ㅠㅠ 제발 여기까지라도ㅠㅠ 제발 여기까지라도ㅠㅠ ");
        return notificationService.subscribe(userId);
    }

    @GetMapping("/receive")
    public int loadNotificationCount(@RequestParam Long userId) {
        return notificationService.loadNotificationCount(userId);
    }

    @GetMapping("/list")
    public List<NotificationDTO> loadNotifications(@RequestParam Long userId) {
        return notificationService.loadNotifications(userId);
    }
}