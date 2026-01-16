package io.d4rkr0n1n.database.controller;

import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.d4rkr0n1n.database.model.Note;
import io.d4rkr0n1n.database.service.NotesResponseService;

@RestController
@RequestMapping("/api/v1")
public class NotesRestController {

    private final NotesResponseService notesResponseService;

    public NotesRestController(NotesResponseService notesResponseService) {
        this.notesResponseService = notesResponseService;
    }

    @GetMapping("/notes")
    public ResponseEntity<List<Note>> getAllNotes() {
        return notesResponseService.retrieveAllNotes();
    }

    @PostMapping("/note/random")
    public ResponseEntity<Note> post() {
        return notesResponseService.createNotesRandom();
    }

    @PostMapping("/note")
    public ResponseEntity<Note> post(@RequestParam String contents) {
        return notesResponseService.createNotes(contents);
    }

    @GetMapping("/note")
    public ResponseEntity<Note> getNote(@RequestParam UUID id) {
        return notesResponseService.retrieveNote(id);
    }

    @PutMapping("/note")
    public ResponseEntity<Note> put(@RequestParam UUID id, @RequestParam String updatedContents) {
        return notesResponseService.updateNote(id, updatedContents);
    }

    @DeleteMapping("/note")
    public ResponseEntity<Note> delete(@RequestParam UUID id) {
        return notesResponseService.deleteNote(id);
    }

}