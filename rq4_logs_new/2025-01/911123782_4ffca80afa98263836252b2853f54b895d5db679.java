package com.roomfit.be.participation.application.event;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public final class JoinAsHostEvent extends ApplicationEvent {
    private final Long userId;
    private final Long chatRoomId;

    private JoinAsHostEvent(Object source, Long userId, Long chatRoomId) {
        super(source);
        this.userId = userId;
        this.chatRoomId = chatRoomId;
    }
    public static JoinAsHostEvent of(Object source,Long userId, Long chatRoomId){
        return new JoinAsHostEvent(source, userId, chatRoomId);
    }
}