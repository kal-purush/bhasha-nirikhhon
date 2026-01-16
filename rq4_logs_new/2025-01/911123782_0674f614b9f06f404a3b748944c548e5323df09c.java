package com.roomfit.be.participation.presentation;

import com.roomfit.be.participation.application.ParticipationService;
import com.roomfit.be.participation.application.event.JoinAsHostEvent;
import com.roomfit.be.participation.application.event.JoinAsParticipantEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * TODO: 추후 Retry 로직 추가 필요
 */
@Component
@RequiredArgsConstructor
public class ParticipationEventListener {
    private final ParticipationService participationService;

    @EventListener
    @Async("taskExecutor")
    public void onJoinAsHostHandler(JoinAsHostEvent event){
        participationService.joinAsHost(event.getUserId(),  event.getChatRoomId());
    }

    @EventListener
    @Async("taskExecutor")
    public void onJoinAsParticipantHandler(JoinAsParticipantEvent event){
        participationService.joinAsParticipant(event.getUserId(),  event.getChatRoomId());
    }
}