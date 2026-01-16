package com.paula9t9.taskmaster;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.view.RedirectView;

import java.util.List;

@Controller
public class TaskController {

    @Autowired
    TaskRepository taskRepository;

    @GetMapping("/tasks")
    public ResponseEntity getTasks(){
        List<Task> results = (List<Task>) taskRepository.findAll();

        return new ResponseEntity(results, HttpStatus.OK);
    }

    @PostMapping("/tasks")
    public RedirectView postTasks(String title, String description){
        Task newTask = new Task(title, description);

        taskRepository.save(newTask);

        return new RedirectView("/tasks");
    }
}