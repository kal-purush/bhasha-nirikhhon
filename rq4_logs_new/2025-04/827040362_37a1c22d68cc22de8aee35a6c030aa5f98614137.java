package org.iromu.ai.utils;

import lombok.experimental.UtilityClass;
import org.iromu.ai.model.ChatRequest;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;

import java.util.List;
import java.util.stream.Collectors;

@UtilityClass
public class RequestUtils {

    public static List<Message> getMessages(ChatRequest request) {
        return request.messages().stream()
                .map(message -> {
                    switch (message.role()) {
                        case "assistant":
                            return new AssistantMessage(message.content());
                        case "user":
                        default:
                            return (Message) new UserMessage(message.content());
                    }
                })
                .collect(Collectors.toList());
    }
}