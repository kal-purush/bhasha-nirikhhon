package kr.ac.kumoh.illdang100.tovalley.dto.notification;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDateTime;
import kr.ac.kumoh.illdang100.tovalley.domain.chat.ChatNotification;
import lombok.AllArgsConstructor;
import lombok.Data;

public class NotificationRespDto {

    @Data
    @AllArgsConstructor
    public static class ChatNotificationRespDto{
        private String chatNotificationId;
        private Long chatRoomId;
        private String senderNick;
        @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Seoul")
        private LocalDateTime createdDate;
        private String content;
        private boolean hasRead;

        public ChatNotificationRespDto(ChatNotification chatNotification) {
            this.chatNotificationId = chatNotification.getId();
            this.chatRoomId = chatNotification.getChatRoomId();
            this.senderNick = chatNotification.getSender().getNickname();
            this.createdDate = chatNotification.getCreatedDate();
            this.content = chatNotification.getContent();
            this.hasRead = chatNotification.getHasRead();
        }
    }
}