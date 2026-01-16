package ru.yandex.practicum.filmorate.controllers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.filmorate.exceptions.GenreNotFoundException;
import ru.yandex.practicum.filmorate.model.Genre;
import ru.yandex.practicum.filmorate.service.FilmService;
import ru.yandex.practicum.filmorate.service.FilmServiceImpl;

import java.util.List;

@Slf4j
@RestController()
@RequestMapping("/genres")
public class GenreController {

    private final FilmService service;

    public GenreController(FilmServiceImpl service) {
        this.service = service;
    }

    @GetMapping("/{id}")
    public Genre getGenreById(@PathVariable int id) {
        if (id < 1 || id > 6) {
            throw new GenreNotFoundException("Genre not found");
        }

        log.info("Genre с ID = {} найден!", id);
        return service.getGenreById(id);
    }

    @GetMapping
    public List<Genre> getAllGenres() {

        return service.getAllGenres();
    }
}