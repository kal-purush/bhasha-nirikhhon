package com.roomfit.be.chat.application.chatroom;

import com.roomfit.be.chat.domain.ChatRoom;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class ChatRoomDTO {

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class Response{
        Long id;
        String name;
        String type;
        String status;
        Integer maxQuota;
        Integer currentQuota;
        public static Response of(ChatRoom chatRoom){
            return Response.builder()
                    .id(chatRoom.getId())
                    .name(chatRoom.getName())
                    .type(chatRoom.getType().name())
                    .status(chatRoom.getStatus().name())
                    .maxQuota(chatRoom.getMaxQuota())
                    .currentQuota(chatRoom.getCurrentQuota())
                    .build();
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class Create{
        String name;
        String type;
        Integer maxQuota;
        Integer currentQuota;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class Enter{

    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class Leave{

    }

}