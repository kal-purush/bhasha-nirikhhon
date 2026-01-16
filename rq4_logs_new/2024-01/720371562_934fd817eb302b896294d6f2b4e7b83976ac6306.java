package com.example.backend.controllers;

import com.example.backend.dtos.ScoreDTO;
import com.example.backend.enums.Game;
import com.example.backend.services.ScoreService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/scores")
@CrossOrigin
public class ScoreController {

    ScoreService scoreService;

    public ScoreController(ScoreService scoreService) {
        this.scoreService = scoreService;
    }

    @GetMapping("/all")
    public ResponseEntity<List<ScoreDTO>> getAllScores() {
        try {
            List<ScoreDTO> scores = scoreService.getAllScores();
            return ResponseEntity.ok(scores);
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }



    @PostMapping("/add/{game}/{scoreValue}")
    public ResponseEntity<String> addScore(@PathVariable Integer game, @PathVariable Integer scoreValue) {
        try {
            String username = SecurityContextHolder.getContext().getAuthentication().getName();
            Game gameEnum = Game.valueOf(game);
            scoreService.createScore(username, gameEnum, scoreValue);
            return ResponseEntity.ok("Score added");
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }
}