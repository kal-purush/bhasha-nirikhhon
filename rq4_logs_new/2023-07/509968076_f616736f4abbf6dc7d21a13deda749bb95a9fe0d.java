package com.flab.football.service.chat;

import com.flab.football.ApiApplication;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest
@RunWith(SpringRunner.class)
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = ApiApplication.class)
@ActiveProfiles("local")
public class ChatServiceImplTest {

    @Autowired
    private ChatService chatService;

    @Test
    @DisplayName("테스트 1")
    public void test1() {
        int channelId = 1;
        int sendUserId = 12;
        String content = "hello world!";
        chatService.sendMessage(channelId, sendUserId, content);
    }

}