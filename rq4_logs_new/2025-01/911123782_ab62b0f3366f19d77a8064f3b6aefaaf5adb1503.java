package com.roomfit.be.chat.domain;

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
    Long id;
    String content;

    @ManyToOne
    @JoinColumn(name = "sender_id")
    User sender;

    @ManyToOne
    @JoinColumn(name = "chatroom_id")
    ChatRoom chatRoom;

    static Message create(User sender, ChatRoom chatRoom, String content){
        return Message.builder()
                .sender(sender)
                .chatRoom(chatRoom)
                .content(content)
                .build();
    }
}