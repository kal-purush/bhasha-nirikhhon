package com.roomfit.be.message.presentation;

import com.roomfit.be.message.application.MessageService;
import com.roomfit.be.message.application.dto.MessageDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

@Controller
@RequiredArgsConstructor
public class MessageEventController {
    private final SimpMessagingTemplate messagingTemplate;
    private final MessageService messageService;

    @MessageMapping("/send")
    public void sendMessage(MessageDTO.Send message) {
        messagingTemplate.convertAndSend("/topic/room/" + message.getRoomId(), message);

        messageService.sendMessage(message);
    }
}