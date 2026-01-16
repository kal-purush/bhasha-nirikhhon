package ru.otus.hw.controllers;


import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import ru.otus.hw.dto.AuthorCreateDto;
import ru.otus.hw.dto.AuthorDto;
import ru.otus.hw.services.AuthorService;


@RestController
@RequiredArgsConstructor
public class AuthorController {

    private final AuthorService authorService;

    @PostMapping("/api/authors/create")
    public ResponseEntity<AuthorDto> save(@RequestBody AuthorCreateDto authorCreateDto) {
        return new ResponseEntity<>(authorService.create(authorCreateDto.getFullName()), HttpStatus.CREATED);
    }
}