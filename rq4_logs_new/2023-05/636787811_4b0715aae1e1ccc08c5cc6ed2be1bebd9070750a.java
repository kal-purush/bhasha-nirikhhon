package com.solvd.elasticsearchmicroservice.kafka.handler.impl;

import com.solvd.elasticsearchmicroservice.kafka.event.NoteEvent;
import com.solvd.elasticsearchmicroservice.kafka.handler.DeleteHandler;
import com.solvd.elasticsearchmicroservice.service.NoteService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DeleteHandlerImpl implements DeleteHandler {

    private final NoteService noteService;

    @Override
    public void handle(NoteEvent event) {
        noteService.delete(event.getId());
    }

}