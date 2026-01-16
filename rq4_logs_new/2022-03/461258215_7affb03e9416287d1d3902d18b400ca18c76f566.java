package ru.hogwarts.school_re.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.hogwarts.school_re.model.Student;

@RestController
@RequestMapping("/getPort")
public class InfoController {

    @Value("${server.port}")
    private String port;

    @GetMapping
    public ResponseEntity getPort() {
        return ResponseEntity.ok(port);
    }
}