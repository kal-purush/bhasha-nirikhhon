package ru.yandex.practicum.filmorate.storage.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.filmorate.exception.NotFoundException;
import ru.yandex.practicum.filmorate.model.Film;
import ru.yandex.practicum.filmorate.storage.FilmStorage;
import ru.yandex.practicum.filmorate.utility.Validator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class InMemoryFilmStorage implements FilmStorage {
    private final Map<Long, Film> films = new HashMap<>();

    @Override
    public Collection<Film> getAllFilms() {
        return films.values();
    }

    @Override
    public Film addFilm(Film film) {
        Validator.validateFilm(film);
        long id = getNextId();
        film.setId(id);
        films.put(id, film);
        log.info("Film with id {} added", id);
        return film;
    }

    @Override
    public Film updateFilm(Film film) {
        Validator.validateFilm(film);
        if (!films.containsKey(film.getId())) {
            log.info("Film with id {} not found", film.getId());
            throw new NotFoundException(String.format("Film with id %d not found", film.getId()));
        }
        films.put(film.getId(), film);
        log.info("Film with id {} updated", film.getId());
        return film;
    }

    private long getNextId() {
        long maxId = films.keySet().stream()
                .max(Long::compareTo)
                .orElse(0L);
        return ++maxId;
    }
}