package ru.yandex.practicum.filmorate.controllers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import ru.yandex.practicum.filmorate.exceptions.ValidationException;
import ru.yandex.practicum.filmorate.model.Film;

import javax.validation.Valid;
import java.time.LocalDate;
import java.util.HashMap;


@RestController
@RequestMapping("/films")
@Slf4j
public class FilmController {
    HashMap <Integer, Film> films = new HashMap<>();
    Integer filmID = 0;

    @GetMapping
    public HashMap <Integer, Film> getAll(){
        log.info("Список фильмов содержит {} записей", films.size());
        return films;
    }

    @PostMapping
    public Film create(@Valid @RequestBody Film film) throws ValidationException {
        film.setId(++filmID);
        if (validationFilm(film)) {
            log.info("Добавлен фильм {}", film);
            films.put(film.getId(), film);
            return film;
        }
        throw new ValidationException("Проблема с данными");
    }

    @PutMapping
    public Film update(@Valid @RequestBody Film film) throws ValidationException {
        if (validationFilm(film)) {
            Integer currentFilmID = film.getId();
            if (films.containsKey(currentFilmID)) {
                log.info("Обновлен фильм {}", film);
                ;
                films.put(film.getId(), film);
                return film;
            } else
                throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        throw new ValidationException("Проблема с данными");
    }

    private boolean validationFilm (Film film) {
        if (film.getName() == null || film.getName().isBlank()) {
            log.debug("Название фильма {} пустое", film);
            return false;
        }
        if (film.getDescription().length() > 200) {
            log.debug("Длина описания фильма {} превышает 200 символов", film);
            return false;
        }
        if (film.getReleaseDate().isBefore(LocalDate.of(1895, 12, 28))) {
            log.debug("Дата релиза фильма {} ранее 28.12.1895", film);
            return false;
        }
        if (film.getDuration() < 0) {
            log.debug("Продолжительность фильма {} отрицательная", film);
            return false;
        }
        return true;
    }
}