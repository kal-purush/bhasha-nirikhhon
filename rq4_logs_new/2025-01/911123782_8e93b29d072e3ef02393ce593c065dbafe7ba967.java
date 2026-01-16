package com.roomfit.be.chat.application.chatroom;

import com.roomfit.be.chat.domain.ChatRepository;
import com.roomfit.be.chat.domain.ChatRoom;
import com.roomfit.be.chat.domain.ChatRoomStatus;
import com.roomfit.be.chat.domain.ChatRoomType;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ChatRoomServiceImpl implements ChatRoomService {

    private final ChatRepository chatRepository;

    public ChatRoomServiceImpl(ChatRepository chatRepository) {
        this.chatRepository = chatRepository;
    }

    @Override
    public ChatRoomDTO.Response createRoom(ChatRoomDTO.Create request) {
        ChatRoom chatRoom = switch (ChatRoomType.fromString(request.getType())) {
            case GROUP -> createGroupRoom(request);
            case PRIVATE -> createPrivateRoom(request);
        };
        ChatRoom savedChatRoom =  chatRepository.save(chatRoom);
        return ChatRoomDTO.Response.of(savedChatRoom);
    }

    @Override
    public ChatRoomDTO.Response enterRoom(ChatRoomDTO.Enter request) {
        return null;
    }

    @Override
    public void leaveRoom(ChatRoomDTO.Leave request) {

    }

    @Override
    public List<ChatRoomDTO.Response> readMessageByUserId(Long userId) {
        return null;
    }
    private ChatRoom createGroupRoom(ChatRoomDTO.Create request){
        return ChatRoom.createGroupRoom(request.getName(), request.getMaxQuota());
    }
    private ChatRoom createPrivateRoom(ChatRoomDTO.Create request){
        return ChatRoom.createPrivateRoom(request.getName());
    }
}