package com.example.demo.service;
import org.springframework.stereotype.Service;
import java.util.List;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class ChatRoomService {

    private final AtomicLong roomIdCounter = new AtomicLong(1);

    public Long createRoom(Long matchingId, List<Long> matchedUsers) {

        System.out.println("✅ CreateChattingRoom - matchingId: " + matchingId +
                           ", users: " + matchedUsers);
        
        // return roomId for test
        Long roomId = roomIdCounter.getAndIncrement();

        return roomId;
    }
}