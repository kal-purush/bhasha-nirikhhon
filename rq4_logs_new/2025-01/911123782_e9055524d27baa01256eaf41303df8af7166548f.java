package com.roomfit.be.participation.application;

import com.roomfit.be.chat.domain.ChatRoom;
import com.roomfit.be.chat.infrastructure.ChatRoomRepository;
import com.roomfit.be.participation.domain.Participation;
import com.roomfit.be.participation.infrastructure.ParticipationRepository;
import com.roomfit.be.user.domain.User;
import com.roomfit.be.user.infrastructure.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ParticipationServiceImpl implements  ParticipationService{
    private final ParticipationRepository participationRepository;
    private final UserRepository userRepository;
    private final ChatRoomRepository chatRoomRepository;

    @Override
    public void joinAsHost(Long userId, Long chatRoomId) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("사용자를 찾을 수 없습니다."));
        ChatRoom chatRoom = chatRoomRepository.findById(chatRoomId)
                .orElseThrow(() -> new IllegalArgumentException("채팅방을 찾을 수 없습니다."));

        Participation participation = Participation.participateAsHost(user, chatRoom);
        participationRepository.save(participation);
    }

    @Override
    public ChatRoom joinAsParticipant(Long userId, Long chatRoomId) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("사용자를 찾을 수 없습니다."));
        ChatRoom chatRoom = chatRoomRepository.findById(chatRoomId)
                .orElseThrow(() -> new IllegalArgumentException("채팅방을 찾을 수 없습니다."));
        
        Participation participation = Participation.participateAsParticipant(user, chatRoom);
        participationRepository.save(participation);
        return chatRoom;
    }

    @Override
    public List<User> readParticipantsByRoomId(Long roomId){
        return participationRepository.findUsersByChatRoomId(roomId);
    }
}