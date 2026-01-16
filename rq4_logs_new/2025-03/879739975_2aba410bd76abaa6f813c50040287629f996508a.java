package com.example.sideproject.domain.notification.service;

import com.example.sideproject.domain.notification.aop.annotation.NotifyOn;
import com.example.sideproject.domain.notification.dto.EventDto;
import com.example.sideproject.domain.notification.dto.EventListDto;
import com.example.sideproject.domain.notification.entity.NotificationType;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ChatNotificationService {

    /**
     * 채팅방 생성 시 알림
     * @param senderName 생성한 유저명
     * @param relatedId 연관된 pk
     * @param receiverId 알림 받을 유저
     */
    @NotifyOn
    public EventListDto createRoom(String senderName, Long relatedId ,Long receiverId) {
        String msg = """
                \'$_senderName\'님과 새로운 채팅이 시작됐어요.
                """
                .replace("$_senderName", senderName);
        EventDto eventDto = EventDto.builder()
                .to(receiverId)
                .msg(msg)
                .relatedId(relatedId)
                .type(NotificationType.CHAT_START)
                .build();
        return new EventListDto(List.of(eventDto));
    }
}