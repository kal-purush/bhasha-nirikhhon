package com.example.backend.controllers;

import com.example.backend.dtos.TaskRecordDto;
import com.example.backend.models.TaskModel;
import com.example.backend.repositories.TaskRepository;
import jakarta.validation.Valid;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping("tasks")
public class TaskController {

    @Autowired
    TaskRepository taskRepository;

    // get com filtro
    // create
    // update
    // delete

    @PostMapping
    public ResponseEntity<TaskModel> create(@RequestBody @Valid TaskRecordDto taskRecordDto) {
        var task = new TaskModel();
        BeanUtils.copyProperties(taskRecordDto, task);
        return ResponseEntity.status(HttpStatus.CREATED).body(taskRepository.save(task));
    }

    @PutMapping("/{id}")
    public ResponseEntity<Object> update(@PathVariable(value="id") UUID id,
                                                @RequestBody @Valid TaskRecordDto taskRecordDto) {
        Optional<TaskModel> taskO = taskRepository.findById(id);
        if(taskO.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Task not found.");
        }
        var task = taskO.get();
        BeanUtils.copyProperties(taskRecordDto, task);
        return ResponseEntity.status(HttpStatus.OK).body(taskRepository.save(task));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Object> delete(@PathVariable(value="id") UUID id) {
        Optional<TaskModel> taskO = taskRepository.findById(id);
        if(taskO.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Task not found.");
        }
        taskRepository.delete(taskO.get());
        return ResponseEntity.status(HttpStatus.OK).body("Task deleted successfully.");
    }
}