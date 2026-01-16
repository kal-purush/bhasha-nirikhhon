package com.example.backend.model;

import java.util.Date;

public record ChatMessageSendDTO(
        String messageId,
        String senderId,
        String recipientId,
        Date timestamp,
        String content
) {
}