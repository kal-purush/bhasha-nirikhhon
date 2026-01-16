package com.itcook.cooking.infra.fcm.converter;

import com.itcook.cooking.domain.domains.infra.fcm.dto.FcmSend;
import com.itcook.cooking.domain.domains.notification.domain.entity.NotificationType;
import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class FollowUserDataPutDataConverter implements NotificationPutDataConverter {

    @Override
    public boolean isSupports(NotificationType notificationType) {
        return notificationType == NotificationType.FOLLOW;
    }

    @Override
    public Map<String, String> getData(FcmSend fcmSend) {
        Map<String, String> data = new HashMap<>();
        data.put("notificationType",NotificationType.FOLLOW.name());
        data.put("followerId", fcmSend.fromUserId().toString());
        return data;
    }
}