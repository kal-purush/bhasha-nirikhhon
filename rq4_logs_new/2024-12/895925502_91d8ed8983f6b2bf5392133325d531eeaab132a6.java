package com.mulmeong.notification.document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Document(collection = "notification_history")
public class NotificationHistory {
    @Id
    private String id;
    private String memberUuid;
    private String notificationId;
    private String kindUuid;
    private String comment;
    private boolean isRead;
    private LocalDateTime createdAt;
}