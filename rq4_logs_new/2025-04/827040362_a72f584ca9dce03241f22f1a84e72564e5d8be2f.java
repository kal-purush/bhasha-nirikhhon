package com.example.ollama.chat;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

@RestController
@CrossOrigin(origins = "*")
@Slf4j
class ChatController {


    private final Chatbot chatbot;

    ChatController(Chatbot chatbot) {
        this.chatbot = chatbot;
    }

    @RequestMapping(value = "api/chat", method = {RequestMethod.POST})
    Flux<String> generateStream(@RequestBody(required = false) Input messageBody) {
        return chatbot.stream(messageBody.id, messageBody.message).map(chat -> chat.getResult().getOutput().getText());
    }

    record Input(String id, String message) {
    }
}