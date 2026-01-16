package ru.yandex.practicum.filmorate.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.filmorate.exception.ValidationException;
import ru.yandex.practicum.filmorate.model.Film;

import javax.validation.Valid;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;


@Slf4j
@RestController
@RequestMapping(value = "/films")
public class FilmController {
    private Integer id;
    private final HashMap<Integer, Film> films;

    public FilmController() {
        id = 0;
        films = new HashMap<>();
    }

    @PostMapping
    public Film create(@Valid @RequestBody Film film) {
        log.info("Пришел запрос на добавление фильма {}", film);
        validate(film);
        film.setId(generateId());
        films.put(id, film);
        log.info("Фильм {} добавлен", film);
        return film;
    }

    @PutMapping
    public Film update(@Valid @RequestBody Film film) {
        log.info("Пришел запрос на обновление фильма {}", film);
        if (films.containsKey(film.getId())) {
            films.put(film.getId(), film);
            log.info("Фильм {} обновлен", film);
            return films.get(film.getId());
        } else {
            RuntimeException e = new ValidationException(
                    String.format("Фильм с идентификатором %d не найден", film.getId()));
            log.info(e.getMessage());
            throw e;
        }
    }

    @GetMapping
    public ArrayList<Film> readAll() {
        return new ArrayList<>(films.values());
    }

    private void validate(Film film) {
        String message;
        if (film.getName().isEmpty()) {
            message = String.format("У фильма %s отсутствует название", film);
            log.info(message);
            throw new ValidationException(message);
        }
        if (film.getDescription().length() > 200) {
            message = String.format("Описание фильма %s более 200 символов", film);
            log.info(message);
            throw new ValidationException(message);
        }
        if (film.getReleaseDate().isBefore(LocalDate.of(1895, 12, 28))) {
            message = String.format("У фильма %s некорректная дата релиза", film);
            log.info(message);
            throw new ValidationException(message);
        }
        if (film.getDuration() <= 0) {
            message = String.format("У фильма %s некорректная продолжительность", film);
            log.info(message);
            throw new ValidationException(message);
        }
    }

    private Integer generateId() {
        return ++id;
    }

}