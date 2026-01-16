package com.gxdxx.instagram.service.message;

import com.gxdxx.instagram.dto.request.MessageListRequest;
import com.gxdxx.instagram.dto.response.MessageListResponse;
import com.gxdxx.instagram.dto.response.MessageResponse;
import com.gxdxx.instagram.entity.ChatRoom;
import com.gxdxx.instagram.entity.User;
import com.gxdxx.instagram.exception.ChatRoomNotFoundException;
import com.gxdxx.instagram.exception.UnauthorizedAccessException;
import com.gxdxx.instagram.exception.UserNotFoundException;
import com.gxdxx.instagram.repository.ChatRoomRepository;
import com.gxdxx.instagram.repository.MessageRepository;
import com.gxdxx.instagram.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Transactional
@RequiredArgsConstructor
@Service
public class MessageQueryService {

    private final MessageRepository messageRepository;
    private final ChatRoomRepository chatRoomRepository;
    private final UserRepository userRepository;

    @Transactional(readOnly = true)
    public MessageResponse findMessages(MessageListRequest request, String nickname) {
        User requestUser = getUserByNickname(nickname);
        ChatRoom chatRoom = chatRoomRepository.findById(request.chatRoomId())
                .orElseThrow(ChatRoomNotFoundException::new);
        if (!chatRoom.hasUser(requestUser)) {
            throw new UnauthorizedAccessException();
        }
        Long cursor = (request.cursor() == null)
                ? messageRepository.findMaxMessageId().map(maxId -> maxId + 1).orElse(0L)
                : request.cursor();
        List<MessageListResponse> messages = messageRepository.getMessagesByCursor(requestUser.getId(), chatRoom.getId(), cursor, 5);
        Long nextCursor = messages.isEmpty() ? 0L : messages.get(messages.size() - 1).getMessageId();

        return MessageResponse.of(nextCursor, messages);
    }

    private User getUserByNickname(String nickname) {
        return userRepository.findByNickname(nickname)
                .orElseThrow(UserNotFoundException::new);
    }

}