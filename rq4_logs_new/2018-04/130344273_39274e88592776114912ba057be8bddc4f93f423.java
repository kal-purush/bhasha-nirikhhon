package com.akai.noder.controllers;

import com.akai.noder.domain.Note;
import com.akai.noder.dto.NoteDto;
import com.akai.noder.exceptions.ResourceNotFoundException;
import com.akai.noder.mappers.NoteMapper;
import com.akai.noder.services.NoteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController(value = "/notes")
public class NoteController extends BaseController {

    private final NoteService noteService;
    private final NoteMapper noteMapper;

    @Autowired
    public NoteController(NoteService noteService, NoteMapper noteMapper) {
        this.noteService = noteService;
        this.noteMapper = noteMapper;
    }

    @GetMapping
    public Page<Note> get(Pageable pageable) {
        return this.noteService.findAll(pageable);
    }

    @GetMapping(value = "/id")
    public NoteDto get(@PathVariable Long id) throws ResourceNotFoundException {
        return this.noteService.findOne(id).map(this.noteMapper::mapToDto)
                .orElseThrow(() -> new ResourceNotFoundException(String.format("Note not found (ID=%d).", id)));
    }

    @PostMapping
    public NoteDto create(@RequestBody NoteDto noteDto) {
        Note note = this.noteMapper.mapToEntity(noteDto);
        return this.noteMapper.mapToDto(this.noteService.create(note));
    }

    @PutMapping(value = "/id")
    public NoteDto update(@PathVariable Long id,
                          @RequestBody NoteDto noteDto) throws ResourceNotFoundException {
        this.noteService.findOne(id).orElseThrow(() -> new ResourceNotFoundException(String.format("Note not found (ID=%d).", id)));
        return this.noteMapper.mapToDto(this.noteService.update(this.noteMapper.mapToEntity(noteDto)));
    }

    @DeleteMapping(value = "/id")
    public void delete(@PathVariable Long id) {
        this.noteService.delete(id);
    }
}