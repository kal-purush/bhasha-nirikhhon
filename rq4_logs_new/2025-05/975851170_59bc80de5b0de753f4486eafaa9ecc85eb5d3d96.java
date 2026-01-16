package com.example.demo.domain;
import java.util.List;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToMany;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
@Entity
public class ChatRoom {
    @Id @GeneratedValue
    private Long id; // 고유 식별자(PK)
    
    private String roomId; // room id(UUID) < 외부 식별자 용
    private int maxUserCount;

    @ManyToMany
    private List<User> participants;

    @Builder
    public ChatRoom(String roomId, int maxUserCount, List<User> participants) {
        this.roomId = roomId;
        this.maxUserCount = maxUserCount;
        this.participants = participants;
    }
}