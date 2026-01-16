package com.anghel.music_band_organizer.services.message;

import com.anghel.music_band_organizer.exceptions.message.MessageAlreadyExistsException;
import com.anghel.music_band_organizer.exceptions.message.MessageNotFoundException;
import com.anghel.music_band_organizer.exceptions.rehearsal.RehearsalAlreadyExistsException;
import com.anghel.music_band_organizer.models.dtos.MessageDTO;
import com.anghel.music_band_organizer.models.entities.Message;
import com.anghel.music_band_organizer.repository.MessageRepository;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class MessageServiceValidationImpl implements MessageServiceValidation{

    private final MessageRepository messageRepository;

    public MessageServiceValidationImpl(MessageRepository messageRepository) { this.messageRepository = messageRepository; }

    @Override
    public void validateMessageAlreadyExists(MessageDTO messageDTO) {
        Message message = messageRepository.findMessageByMessageDateAndMessageTime(messageDTO.getMessageDate(), messageDTO.getMessageTime());

        if (message != null) {
            throw new MessageAlreadyExistsException("A rehearsal with the date " + messageDTO.getMessageDate() + " and time " + messageDTO.getMessageTime() + " already exists.");
        }
    }

    @Override
    public Message getValidMessage(Long messageId, String methodName) {
        Message message = messageRepository.findById(messageId)
                .orElseThrow(() -> new MessageNotFoundException("Message with id " + messageId + "not found."));
        log.info("Message with id {} retrieved. Method: {}.", messageId, methodName);

        return message;
    }
}