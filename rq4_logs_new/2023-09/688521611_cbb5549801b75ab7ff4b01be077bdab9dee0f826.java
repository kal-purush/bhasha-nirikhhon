package com.example.backend.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Document(collection = "chat_sessions")
public record ChatSession(
    @Indexed(unique = true, name = "_unique_session_identifier_")
    String uniqueSessionIdentifier,
    @DBRef
    @JsonIgnoreProperties("chatSession")
    List<ChatMessage> chat_messages

) {
}