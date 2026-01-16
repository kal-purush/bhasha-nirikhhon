package com.todolist.controllers;

import com.todolist.models.Task;
import com.todolist.services.TaskService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class ToDoController {

    private final TaskService taskService;

    public ToDoController(TaskService taskService) {
        this.taskService = taskService;
    }

    @GetMapping(value = "/tasks")
        public List<Task> getTasks () {
            return taskService.getTasks();
        }


    @PostMapping(value = "/tasks")
        public Task saveTask (@RequestBody Task task){
            return taskService.saveTask(task);
        }


}