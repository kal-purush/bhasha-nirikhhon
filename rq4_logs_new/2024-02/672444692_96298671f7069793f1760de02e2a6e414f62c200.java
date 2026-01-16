package kr.ac.kumoh.illdang100.tovalley.dto.chat;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDateTime;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

public class ChatRespDto {

    @Data
    @AllArgsConstructor
    public static class CreateNewChatRoomRespDto {
        private boolean isNew;
        private Long chatRoomId;
    }

    @Data
    public static class ChatRoomRespDto {
        private Long chatRoomId;
        private String chatRoomTitle;
        private String otherUserProfileImage;
        private String otherUserNick;
        private String lastMessageContent;
        @JsonFormat(pattern = "yyyy-MM-dd hh:mm:ss", timezone = "Asia/Seoul")
        private LocalDateTime lastMessageTime;

        public void changeLastMessage(String lastMessageContent, LocalDateTime lastMessageTime) {
            this.lastMessageContent = lastMessageContent;
            this.lastMessageTime = lastMessageTime;
        }
    }

    @Data
    public static class ChatMessageListRespDto{
        private Long chatRoomId;
        private List<ChatMessageRespDto> chatMessages;
    }

    @Data
    public static class ChatMessageRespDto {
        private String chatMessageId;
        private Long senderId;
        private boolean isMyMsg;
        private String content;
        @JsonFormat(pattern = "yyyy-MM-dd hh:mm:ss", timezone = "Asia/Seoul")
        private LocalDateTime createdAt;
        private int reaCount;
    }
}