package com.example.spoot_taxi_front.utils;

import com.example.spoot_taxi_front.models.ChatMessage;
import com.example.spoot_taxi_front.models.ChatRoom;

import java.util.ArrayList;
import java.util.List;

public class ChatRoomMetaInformationManager {

    private static ChatRoomMetaInformationManager instance;

    private List<ChatRoom> chatRooms = new ArrayList<>();

    private ChatRoomMetaInformationManager() {}

    public static ChatRoomMetaInformationManager getInstance() {
        if (instance == null) {
            synchronized (ChatRoomMetaInformationManager.class) {
                if (instance == null) {
                    instance = new ChatRoomMetaInformationManager();
                }
            }
        }
        return instance;
    }

    public List<ChatRoom> getChatRooms() {
        return chatRooms;
    }
    public void setChatRooms(List<ChatRoom> chatRooms) {
        this.chatRooms = chatRooms;
    }

    public void newMessageArriveUpdate(ChatMessage chatMessage) {

        Long chatRoomId = chatMessage.getChatRoomId();
        String message = chatMessage.getMessage();
        String sentTime = chatMessage.getSentTime();

        for (int i = 0; i < chatRooms.size(); i++) {
            ChatRoom chatRoom = chatRooms.get(i);
            if (chatRoom.getRoomId() == chatRoomId) {
                if (!chatMessage.getSenderId().equals(SessionManager.getInstance().getCurrentUser().getEmail())) {
                    chatRoom.setNonReadMessageCount(chatRoom.getNonReadMessageCount() + 1);
                }
                chatRoom.setLastMessage(message);
                chatRoom.setLastSentTime(sentTime);
                break;
            }
        }
    }

    public void nonReadCountZeroUpdate(Long chatRoomId) {
        for (int i = 0; i < chatRooms.size(); i++) {
            ChatRoom chatRoom = chatRooms.get(i);
            if (chatRoom.getRoomId() == chatRoomId) {
                chatRoom.setNonReadMessageCount(0);
                break;
            }
        }
    }

}