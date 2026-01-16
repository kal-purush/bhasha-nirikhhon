package com.roomfit.be.message.domain;

import com.roomfit.be.chat.domain.ChatRoom;
import com.roomfit.be.global.entity.BaseEntity;
import com.roomfit.be.user.domain.User;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity(name = "messages")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Message extends BaseEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String content;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "sender_id", insertable = false, updatable = false)
    private User sender;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "chatroom_id", insertable = false, updatable = false)
    private ChatRoom chatRoom;

    @Column(name = "sender_id")
    private Long senderId;

    @Column(name = "chatroom_id")
    private Long chatRoomId;

    public static Message create(Long senderId, Long chatRoomId, String content) {
        return Message.builder()
                .content(content)
                .senderId(senderId)
                .chatRoomId(chatRoomId)
                .build();
    }
}