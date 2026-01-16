package com.roomfit.be.message.application.dto;

import com.roomfit.be.message.domain.Message;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class MessageDTO {
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Response{
        Long id;
        String content;
        String sender;
        public static Response of(Message message){
            return Response.builder()
                    .id(message.getId())
                    .content(message.getContent())
                    .sender(message.getSender().getNickname())
                    .build();
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Send{
        Long roomId;
        Long userId;
        String content;
    }

}