package com.web.stard.chat_stomp;

import com.web.stard.domain.BaseEntity;
import com.web.stard.domain.Member;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import javax.validation.constraints.NotNull;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
public class ChatMessage extends BaseEntity {

    public ChatMessage(String message) {
        this.type = MessageType.GREETING;
        this.message = message;
    }

    public enum MessageType {
        GREETING, TALK
    }

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @NotNull
    @Enumerated(EnumType.STRING)
    private MessageType type;

    @NotNull
    private Long studyId;   //채팅방의 스터디 ID

    @ManyToOne @NotNull
    @JoinColumn(name = "member_id") //보내는 사람
    private Member member;

    @NotNull
    private String message; //내용

}