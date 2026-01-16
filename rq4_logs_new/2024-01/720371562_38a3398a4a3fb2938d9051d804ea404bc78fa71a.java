package com.example.backend.controllers;

import com.example.backend.models.TypingEntity;
import com.example.backend.services.TypingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/typing")
@CrossOrigin
public class TypingController {

    @Autowired
    private TypingService typingService;

    @GetMapping("/words")
    public ResponseEntity<List<TypingEntity>> loadAllWords() {
        try {
            List<TypingEntity> words = typingService.loadAllWords();
            return ResponseEntity.ok(words);
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/word/{id}")
    public ResponseEntity<Optional<TypingEntity>> loadWord(@PathVariable Long id) {
        try {
            Optional<TypingEntity> word = typingService.loadWordById(id);
            return ResponseEntity.ok(word);
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

}