package com.spring.mood.projectmvc.service;

import com.spring.mood.projectmvc.entity.ChatRoom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
@Transactional
class ChatServiceTest {

    @Autowired
    private ChatService chatService;

    @Test
    @DisplayName("asdsad")
    void setChatService() {
        //given
        Optional<ChatRoom> chatRoomByTopicAndRoomId = chatService.findChatRoomByTopicAndRoomId(1, 2);

        //when
        assertTrue(chatRoomByTopicAndRoomId.isPresent());

        //then
    }


}