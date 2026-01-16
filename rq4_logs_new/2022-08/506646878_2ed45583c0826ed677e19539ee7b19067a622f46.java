package ru.yandex.practicum.filmorate.controller;


import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import ru.yandex.practicum.filmorate.exceptions.NotFoundException;
import ru.yandex.practicum.filmorate.model.Genre;
import ru.yandex.practicum.filmorate.service.GenreService;


import javax.validation.constraints.Min;
import java.util.Collection;

@RestController
@RequiredArgsConstructor
public class GenreController {
   private final GenreService genreService;

    @GetMapping("/genres")
    public Collection<Genre> getAll() {
        return genreService.getAll();
    }

    @GetMapping("/genres/{id}")
    public Genre getById(@PathVariable @Min(1) int id) throws NotFoundException {
       return genreService.getById(id);
    }
}