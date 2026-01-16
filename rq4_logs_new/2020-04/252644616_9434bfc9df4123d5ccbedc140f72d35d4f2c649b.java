package com.iessanvicente.springboot.backend.chat.models.services;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.iessanvicente.springboot.backend.chat.models.dao.MessageRepository;
import com.iessanvicente.springboot.backend.chat.models.documents.Message;

@Service
public class MessageServiceImpl implements IMessageService {

	@Autowired
	private MessageRepository messageRepository;
	
	@Override
	public List<Message> getLast10Messages() {
		return messageRepository.findFirst10ByOrderByDateDesc();
	}

	@Override
	public Message save(Message message) {
		return messageRepository.save(message);
	}
	
}