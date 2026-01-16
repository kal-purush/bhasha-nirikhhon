package org.aprikhodskiy.otus.controllers;

import lombok.AllArgsConstructor;
import org.aprikhodskiy.otus.dto.*;
import org.aprikhodskiy.otus.services.AuthorService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@AllArgsConstructor
@RestController
public class AuthorController {

    private final AuthorService authorService;

    @RequestMapping("/authors")
    public ResponseEntity<List<AuthorDto>> authors() {
        return ResponseEntity.status(HttpStatus.OK).body(
                authorService.findAllAuthors()
        );
    }
}