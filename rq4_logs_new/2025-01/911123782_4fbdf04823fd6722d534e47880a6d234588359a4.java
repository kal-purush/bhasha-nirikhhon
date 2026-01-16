package com.roomfit.be.message.application;

import com.roomfit.be.message.application.dto.MessageDTO;
import com.roomfit.be.message.domain.Message;
import com.roomfit.be.message.infrastructure.MessageRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class MessageServiceImpl implements  MessageService{
    private final MessageRepository messageRepository;

    @Override
    public List<MessageDTO.Response> readMessage() {
        List<Message> foundMessage =  messageRepository.findAll();
        return foundMessage.stream()
                .map(MessageDTO.Response::of)
                .toList();
    }

    @Override
    public MessageDTO.Response sendMessage(MessageDTO.Send request) {
        Long senderId = request.getUserId();
        Long roomId = request.getRoomId();
        String content = request.getContent();
        Message newMessage = Message.create(senderId, roomId, content);
        Message savedMessage = messageRepository.save(newMessage);
        return MessageDTO.Response.of(savedMessage);
    }

    public List<MessageDTO.Response> saveBulkMessage(List<MessageDTO.Send> request) {
        return null;
    }


}