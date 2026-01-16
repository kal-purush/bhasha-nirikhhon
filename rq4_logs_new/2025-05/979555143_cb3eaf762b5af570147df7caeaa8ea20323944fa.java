package com.kanban.spring.controller;

import com.kanban.spring.controller.Task;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@Controller
public class HomeController {

    @GetMapping("/")
    public String showKanbanBoard(Model model) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Task> tasks = null;
        try {
            tasks = objectMapper.readValue(Paths.get("src/main/resources/tasks.json").toFile(), new TypeReference<List<Task>>() {});
        } catch (IOException e) {
            e.printStackTrace();
        }
        model.addAttribute("tasks", tasks);
        return "index"; // Index-Seite wird geladen
    }
}