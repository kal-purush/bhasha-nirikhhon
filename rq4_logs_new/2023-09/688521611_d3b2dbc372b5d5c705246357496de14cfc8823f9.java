package com.example.backend.controller;

import com.example.backend.service.ChatSessionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/chatsessions")
@RequiredArgsConstructor
public class ChatSessionController {

    private final ChatSessionService chatSessionService;

    @PostMapping("/{participantOneId}/{participantTwoId}")
    @ResponseStatus(HttpStatus.CREATED)
    public void addChatSession(@PathVariable("participantOneId") String participantOneId, @PathVariable("participantTwoId") String participantTwoId)
    {

    }

}