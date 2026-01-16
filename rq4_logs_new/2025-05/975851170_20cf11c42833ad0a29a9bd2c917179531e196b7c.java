package com.example.demo.controller;

import com.example.demo.dto.ChatMessageDto;
import com.example.demo.security.CustomUserDetails;
import com.example.demo.service.WebSocketChatService;

import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;

import java.security.Principal;
import lombok.RequiredArgsConstructor;

@Controller
@RequiredArgsConstructor
public class WebSocketController {

    private final WebSocketChatService chatService;

    @MessageMapping("/chatroom.{roomId}")
    public void handleMessage(@DestinationVariable String roomId,
                              @Payload ChatMessageDto message,
                              Principal principal) {

    /**
     * WebSocket은 HTTP가 아니라 STOMP 메시지 기반이라 Spring MVC의 ArgumentResolver 미작동
     * 따라서 WebSocket 메시지에서는 여전히 Principal(Spring Security가 주입하는 Authentication 객체)로 받되 
     * 내부적으로는 CustomUserDetails로 캐스팅하여 일관성 유지(가독성, 유지보수, 확장성)
     */
     
    if (!(principal instanceof Authentication)) {
        throw new RuntimeException("인증 정보 없음");
    }

    Authentication authentication = (Authentication) principal;
    CustomUserDetails userDetails = (CustomUserDetails) authentication.getPrincipal();
    Long userId = userDetails.getId();

    System.out.println("[DEBUG] WebSocket 메시지 수신: roomId=" + roomId + ", content=" + message.getContent());
    System.out.println("[DEBUG] From userId=" + userId);


    chatService.processMessage(roomId, message, userId);
    }
}