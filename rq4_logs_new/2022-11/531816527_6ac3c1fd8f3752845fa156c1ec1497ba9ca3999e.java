package ru.otus.projs.hw11.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.otus.projs.hw11.model.Author;
import ru.otus.projs.hw11.repository.AuthorRepository;
import ru.otus.projs.hw11.repository.BookRepository;
import ru.otus.projs.hw11.service.AuthorService;

@RestController
@RequiredArgsConstructor
public class AuthorController {

    private final AuthorService authorService;
    private final AuthorRepository authorRepository;
    private final BookRepository bookRepository;

    @GetMapping(path = "/api/author")
    public Flux<Author> authorList() {
        return authorRepository.findAll();
    }

    @GetMapping(path = "/api/author/{id}")
    public Mono<Author> getAuthor(@PathVariable("id") String id) {
        return authorRepository.findById(id);
    }

    @PostMapping(path = "/api/author")
    public Mono<ResponseEntity<Author>> saveAuthor(@RequestBody Author author) {
        return authorRepository.save(authorService.prepareAuthor4Save(author))
                .map(ResponseEntity::ok);
    }

    @DeleteMapping(path = "/api/author/{id}")
    public Mono<ResponseEntity<Void>> deleteAuthor(@PathVariable("id") String authorId) {
        return bookRepository.countByGenreId(authorId)
                .filter(id -> id == 0)
                .flatMap(cnt -> authorRepository.deleteById(authorId).map(ResponseEntity::ok))
                .switchIfEmpty(Mono.fromCallable(()->ResponseEntity.unprocessableEntity().build()));
    }

}