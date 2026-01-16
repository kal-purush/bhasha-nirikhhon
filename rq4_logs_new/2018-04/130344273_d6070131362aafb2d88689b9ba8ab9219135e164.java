package com.akai.noder.services.impl;

import com.akai.noder.domain.Note;
import com.akai.noder.repositories.NoteRepository;
import com.akai.noder.services.NoteService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class NoteServiceImpl implements NoteService {

    private final NoteRepository noteRepository;

    public NoteServiceImpl(NoteRepository noteRepository) {
        this.noteRepository = noteRepository;
    }

    @Override
    public Page<Note> findAll(Pageable pageable) {
        return this.noteRepository.findAll(pageable);
    }

    @Override
    public Optional<Note> findOne(Long id) {
        return Optional.ofNullable(this.noteRepository.findOne(id));
    }

    @Override
    @Transactional
    public Note create(Note note) {
        return this.noteRepository.save(note);
    }

    @Override
    @Transactional
    public Note update(Note note) {
        return this.noteRepository.save(note);
    }

    @Override
    public void delete(Long id) {
        this.noteRepository.delete(id);
    }
}