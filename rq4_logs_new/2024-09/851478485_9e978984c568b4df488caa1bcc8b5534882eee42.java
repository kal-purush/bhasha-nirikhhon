package org.detective.controller.chat;

import lombok.RequiredArgsConstructor;
import org.detective.entity.Chat;
import org.detective.entity.ChatRoom;
import org.detective.repository.ChatRepository;
import org.detective.services.chat.ChatService;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RequiredArgsConstructor
@RestController
public class ChatController {

    private final ChatService chatService;
    private final ChatRepository chatRepository;

    // /receive를 메시지를 받을 endpoint로 설정합니다.    // /send로 메시지를 반환합니다.
    @MessageMapping("/receive")
    @SendTo("/send")
    public Chat SocketHandler(Chat chatMessage) {
        return chatService.saveMessage(chatMessage);
    }

    @GetMapping("/chat/chatroom/{chatRoomId}")
    public List<Chat> getMessages(@PathVariable String chatRoomId) {
        // 채팅방 ID에 해당하는 모든 메시지 반환
        return chatService.getMessageFromRoomId(chatRoomId);
    }


}
