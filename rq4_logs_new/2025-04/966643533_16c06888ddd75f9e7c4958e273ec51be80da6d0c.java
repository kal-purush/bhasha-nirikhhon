package org.spiceboys.Travel.Diary.controller;

import org.spiceboys.Travel.Diary.model.Note;
import org.spiceboys.Travel.Diary.service.NoteService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class NoteController {
    private final NoteService noteService;

    public NoteController(NoteService noteService) {
        this.noteService = noteService;
    }

    @GetMapping("/api/activities/{activityId}/notes")
    public ResponseEntity<List<Note>> getNotesByActivityId(@PathVariable("activityId") Long activityId) {
        List<Note> Notes = noteService.getNotesByActivityId(activityId);
        return ResponseEntity.status(HttpStatus.OK).body(Notes);
    }

    @PostMapping("/api/notes")
    public ResponseEntity<Note> createNote(@RequestBody Note note) {
        Note createdNote = noteService.createNote(note);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdNote);
    }

    @PatchMapping("/api/notes/{noteId}")
    public ResponseEntity<Note> updateNote(@PathVariable Long noteId, @RequestBody Note note) {
        Note updatedNote = noteService.updateNote(noteId, note);
        return ResponseEntity.status(HttpStatus.OK).body(updatedNote);
    }

    @DeleteMapping("/api/notes/{noteId}")
    public ResponseEntity<Note> deleteNote(@PathVariable Long noteId) {
        noteService.deleteNote(noteId);
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
}