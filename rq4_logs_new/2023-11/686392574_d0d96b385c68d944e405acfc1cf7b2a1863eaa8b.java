package ru.yandex.practicum.filmorate.exceptions.mpa;

public class MpaNotFoundException extends RuntimeException {
    public MpaNotFoundException(String message) {
        super(message);
    }
}