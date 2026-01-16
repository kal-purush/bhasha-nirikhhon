package com.solvd.elasticsearchmicroservice.kafka;

import com.solvd.elasticsearchmicroservice.domain.Note;
import com.solvd.elasticsearchmicroservice.kafka.event.NoteEvent;
import com.solvd.elasticsearchmicroservice.service.NoteService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KfConsumerImpl implements KfConsumer{

    private final NoteService noteService;

    @KafkaListener(topics = "${kafka.topic}", groupId = "groupId", containerFactory = "kafkaListenerContainerFactory")
    public void receive(NoteEvent event) {
        log.info("Message received -> {}", event);
        switch (event.getType()) {
            case PUT -> update(generateNote(event));
            case POST -> create(generateNote(event));
            case DELETE -> delete(event.getId());
        }
    }

    @Override
    public void create(Note note) {
        noteService.create(note);
    }

    @Override
    public void update(Note note) {
        noteService.update(note);
    }

    @Override
    public void delete(Long id) {
        noteService.delete(id);
    }

    private Note generateNote(NoteEvent event){
        Note note = new Note();
        note.setId(event.getId());
        note.setTag(event.getTag());
        note.setDescription(event.getDescription());
        note.setTheme(event.getTheme());
        note.setUserId(event.getUserId());
        return note;
    }

}