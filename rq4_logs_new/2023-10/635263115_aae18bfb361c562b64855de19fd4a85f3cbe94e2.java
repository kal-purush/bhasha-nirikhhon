package ru.yandex.practicum.filmorate.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.filmorate.models.Genre;
import ru.yandex.practicum.filmorate.service.GenreService;

import javax.validation.Valid;
import java.util.List;

@RestController
@RequestMapping("/genres")
public class GenreController {

    private final GenreService service;

    @Autowired
    public GenreController(GenreService service) {
        this.service = service;
    }

    @GetMapping("/{id}")
    public Genre findById(@PathVariable int id) {
        return service.findById(id);
    }

    @GetMapping
    public List<Genre> findAll() {
        return service.findAll();
    }

    @PostMapping
    public Genre create(@Valid @RequestBody Genre data) {
        return service.create(data);
    }

    @PutMapping
    public Genre update(@Valid @RequestBody Genre data) {
        return service.update(data);
    }

    @DeleteMapping("/{id}")
    public void delete(@PathVariable int id) {
        service.delete(id);
    }
}