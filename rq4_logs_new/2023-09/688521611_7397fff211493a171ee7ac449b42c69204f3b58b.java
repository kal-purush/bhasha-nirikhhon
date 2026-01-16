package com.example.backend.security;

import com.example.backend.model.ChatSession;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

import java.util.List;

@Document(collection = "users")
public record AppUser(
        @MongoId
        String userId,
        @Indexed(unique = true, name = "_user_name_")
        String userName,
        String password,
        AppUserRole role,
        @DBRef
        List<ChatSession> chat_sessions,
        boolean isVirtualAgent,
        String api,
        String apiKey
) {
}