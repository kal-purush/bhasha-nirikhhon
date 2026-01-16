package com.fitastic.controller;

import com.fitastic.entity.DefaultExercise;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import com.fitastic.service.DefaultExerciseService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@AllArgsConstructor
@RequestMapping("/api/defaultExercises")
public class DefaultExerciseController {

    private DefaultExerciseService exerciseService;

    @GetMapping("/all")
    public List<DefaultExercise> getExercises() {
        return exerciseService.getAll();
    }

    @GetMapping("/{id}")
    public ResponseEntity<DefaultExercise> getExercise(@PathVariable String id) {
        try{
            DefaultExercise exercise = exerciseService.getExerciseById(id);
            return ResponseEntity.ok(exercise);
        } catch (RuntimeException e) {
            return ResponseEntity.notFound().build();
        }
    }
}