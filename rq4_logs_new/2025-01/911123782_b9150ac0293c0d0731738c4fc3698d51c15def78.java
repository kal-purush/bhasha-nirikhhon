package com.roomfit.be;

import com.roomfit.be.chat.domain.ChatRoom;
import com.roomfit.be.chat.infrastructure.ChatRoomRepository;
import com.roomfit.be.message.application.MessageService;
import com.roomfit.be.message.application.dto.MessageDTO;
import com.roomfit.be.user.domain.User;
import com.roomfit.be.user.infrastructure.UserRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
@Slf4j
public class MessageTest {

    @Autowired
    private MessageService messageService;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private ChatRoomRepository chatRoomRepository;
    private User testUser;
    private ChatRoom testChatRoom;
    @BeforeEach
    void setUp(){
        testUser = userRepository.save(createUser());
        testChatRoom = chatRoomRepository.save(createChatRoom());
    }
    private User createUser(){
        return User.createUser("nickname", "email12","password12");
    }
    private ChatRoom createChatRoom(){
        return ChatRoom.createGroupRoom("room1",4);
    }
    @Test
    void 메시지_서비스_생성(){
        log.info(testUser.toString());
        log.info(testChatRoom.toString());
        MessageDTO.SenderDTO senderDTO = new MessageDTO.SenderDTO(testUser.getId(), testUser.getNickname());
        MessageDTO.Send request =new MessageDTO.Send(testChatRoom.getId(),senderDTO,"content");
        MessageDTO.Response response = messageService.sendMessage(request);
        List<ChatRoom> chatRoom = chatRoomRepository.findAll();
        log.info(response.toString());
    }
}