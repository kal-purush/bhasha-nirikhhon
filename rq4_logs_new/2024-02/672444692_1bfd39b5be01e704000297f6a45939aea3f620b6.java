package kr.ac.kumoh.illdang100.tovalley.domain.notification;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;

public interface ChatNotificationRepositoryCustom {
    Slice<ChatNotification> findSliceChatNotificationsByMemberId(Long memberId, Pageable pageable);
}