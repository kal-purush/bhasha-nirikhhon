package com.example.librarymanagement.controller;

import com.example.librarymanagement.dto.AuthorDto;
import com.example.librarymanagement.dto.mapper.AuthorMapper;
import com.example.librarymanagement.model.entity.Author;
import com.example.librarymanagement.service.AuthorService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/author")
public class AuthorController {
    private final AuthorService authorService;

    public AuthorController(AuthorService authorService) {
        this.authorService = authorService;
    }

    @GetMapping("/all")
    public ResponseEntity<List<AuthorDto>> getAllAuthors() {
        return new ResponseEntity<>(AuthorMapper.toAuthorDto(authorService.getAllAuthors()), HttpStatus.OK);
    }

    @GetMapping
    public ResponseEntity<AuthorDto> getAuthorById(@RequestParam Long id) {
        return new ResponseEntity<>(new AuthorDto(authorService.getAuthorById(id)), HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<AuthorDto> createAuthor(@RequestBody @Valid Author author) {
        return new ResponseEntity<>(new AuthorDto(authorService.createAuthor(author)), HttpStatus.CREATED);
    }

    @PutMapping
    public ResponseEntity<AuthorDto> updateAuthor(@RequestParam Long id, @RequestBody @Valid Author author) {
        return new ResponseEntity<>(new AuthorDto(authorService.updateAuthor(id, author)), HttpStatus.OK);
    }
}