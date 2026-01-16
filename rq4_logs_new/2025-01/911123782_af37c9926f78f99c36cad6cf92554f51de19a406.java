package com.roomfit.be.participation.domain;

import com.roomfit.be.chat.domain.ChatRoom;
import com.roomfit.be.user.domain.User;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

/**
 * 완화
 */
@Entity(name = "participation")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Participation {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "user_id", nullable = false)
    private User user;

    @ManyToOne
    @JoinColumn(name = "chat_room_id", nullable = false)
    private ChatRoom chatRoom;

    private ParticipationType type;

    public static Participation participateAsHost(User user, ChatRoom chatRoom) {
        return Participation.builder()
                .user(user)
                .chatRoom(chatRoom)
                .type(ParticipationType.HOST)
                .build();
    }

    public static Participation participateAsParticipant(User user, ChatRoom chatRoom) {
        return Participation.builder()
                .user(user)
                .chatRoom(chatRoom)
                .type(ParticipationType.PARTICIPANT)
                .build();
    }
}