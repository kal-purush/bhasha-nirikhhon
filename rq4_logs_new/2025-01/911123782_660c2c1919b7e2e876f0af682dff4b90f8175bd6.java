package com.roomfit.be.participation.application.event;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public final class JoinAsParticipantEvent extends ApplicationEvent {
    private final Long userId;
    private final Long chatRoomId;

    private JoinAsParticipantEvent(Object source, Long userId, Long chatRoomId) {
        super(source);
        this.userId = userId;
        this.chatRoomId = chatRoomId;
    }
    public static JoinAsParticipantEvent of(Object source,Long userId, Long chatRoomId){
        return new JoinAsParticipantEvent(source, userId, chatRoomId);
    }
}