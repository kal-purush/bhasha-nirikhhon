package com.example.demo.service;

import com.example.demo.domain.ChatRoom;
import com.example.demo.domain.User;
import com.example.demo.dto.ChatMessageDto;
import com.example.demo.repository.ChatRoomRepository;
import com.example.demo.repository.UserRepository;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class WebSocketChatService {
    
    private final SimpMessagingTemplate messagingTemplate;
    private final ChatRoomRepository chatRoomRepository;
    private final UserRepository userRepository;

    // readOnly = true 조건은 추후 메시지나 로그 저장이 필요하면 삭제
    @Transactional(readOnly = true) 
    public void processMessage(String roomId, ChatMessageDto message, Long userId) {
        User user = userRepository.findById(userId)
            .orElseThrow(() -> new RuntimeException("사용자 없음"));
        
        // 클라이언트에게 보여지는 데이터
        String username = user.getName();

        ChatRoom room = chatRoomRepository.findByRoomId(roomId)
            .orElseThrow(() -> new RuntimeException("채팅방 없음"));

        // 비인가 사용자 메시지 차단
        boolean isParticipant = room.getParticipants().stream()
            .anyMatch(u -> u.getId().equals(user.getId()));

        if (!isParticipant) {
            System.out.println("[WARN] 비인가 유저의 메시지 시도: " + username);
            return;
        }

        message.setSender(username);
        messagingTemplate.convertAndSend("/sub/chat/room/" + roomId, message);        
    }
    

}