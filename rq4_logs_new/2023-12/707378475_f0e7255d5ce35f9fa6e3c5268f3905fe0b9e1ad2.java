package com.gitlab.tomaszgryczka.fungiseeker.application;


import com.gitlab.tomaszgryczka.fungiseeker.domain.chat.ChatMessage;
import com.gitlab.tomaszgryczka.fungiseeker.domain.chat.ChatMessageService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@RequiredArgsConstructor
@RequestMapping("/chat")
@RestController
public class ChatController {

    private final ChatMessageService chatMessageService;


    @GetMapping("/messages/{mushroomHuntingId}")
    public Collection<ChatMessage> getAllMessages(@PathVariable Long mushroomHuntingId) {
        return chatMessageService.getAllChatMessagesForMushroomHunting(mushroomHuntingId);
    }
}