package com.mulmeong.notification.application;

import com.mulmeong.notification.common.utils.CursorPage;
import com.mulmeong.notification.document.NotificationHistory;
import com.mulmeong.notification.dto.NotificationHistoryResponseDto;
import com.mulmeong.notification.dto.NotificationStatusResponseDto;

import java.util.List;

public interface NotificationService {
    CursorPage<NotificationHistoryResponseDto> getNotificationHistoryByPage(
            String memberUuid,
            String type,
            String kind,
            String lastId,
            Integer pageSize,
            Integer pageNo);

    List<NotificationStatusResponseDto> getAllNotificationStatus(String memberUuid);

    void updateNotificationStatus(String memberUuid, String notificationType);

    void updateNotificationHistoryRead(String memberUuid, String historyUuid);

}