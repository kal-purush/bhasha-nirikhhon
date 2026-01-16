package com.roomfit.be.message.presentation;

import com.roomfit.be.message.application.MessageService;
import com.roomfit.be.message.application.dto.MessageDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/room/{room_id}")
@RequiredArgsConstructor
public class MessageController {
    private final MessageService messageService;

    @GetMapping("/messages")
    public List<MessageDTO.Response> readMessage(@PathVariable Long room_id) {
        return messageService.readMessage();
    }
}