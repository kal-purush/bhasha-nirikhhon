package ru.yandex.practicum.filmorate.utility;

import ru.yandex.practicum.filmorate.exception.ValidationException;
import ru.yandex.practicum.filmorate.model.Film;

import java.time.LocalDate;
import java.time.Month;

public class Validator {

    public static void validateFilm(Film film) {
        if (film.getName() == null || film.getName().isBlank()) {
            throw new ValidationException("Film name cannot be empty");
        }
        if (film.getDescription().length() > 200) {
            throw new ValidationException("Film description cannot be longer than 200 characters");
        }
        if (film.getReleaseDate().isBefore(LocalDate.of(1895, Month.DECEMBER, 28))) {
            throw new ValidationException("Film release date cannot be earlier than 28.12.1895");
        }
        if (!film.getDuration().isPositive()) {
            throw new ValidationException("Film duration should be positive");
        }
    }
}