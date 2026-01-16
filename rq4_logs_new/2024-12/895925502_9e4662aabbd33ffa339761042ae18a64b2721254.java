package com.mulmeong.notification.application;

import com.mulmeong.notification.document.NotificationHistory;
import com.mulmeong.notification.document.NotificationStatus;
import com.mulmeong.notification.document.NotificationType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

public interface SseService {
    SseEmitter subscribe(String memberUuid, String lastEventId);

    void sendNotificationToClient(SseEmitter emitter, String eventId, String emitterId, Object data);

    void send(NotificationHistory notificationHistory);
}