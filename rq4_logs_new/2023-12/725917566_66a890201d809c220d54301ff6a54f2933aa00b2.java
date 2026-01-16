package ru.yandex.practicum.filmorate.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.filmorate.exception.ValidationException;
import ru.yandex.practicum.filmorate.model.Film;
import ru.yandex.practicum.filmorate.model.User;
import ru.yandex.practicum.filmorate.storage.FilmStorage;
import ru.yandex.practicum.filmorate.storage.UserStorage;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class FilmService {

    private final FilmStorage filmStorage;
    private final UserStorage userStorage;

    public FilmService(FilmStorage filmStorage, UserStorage userStorage) {
        this.filmStorage = filmStorage;
        this.userStorage = userStorage;
    }

    public void addLike(Integer filmId, Integer userId) {

        Film film = filmStorage.getFilmById(filmId);
        User user = userStorage.getUserById(userId);

        if (film != null && user != null) {
            film.getLikes().add(userId);
        } else {
            throw new ValidationException("Фильма или пользователя с указанным id не существует");
        }
    }


    public void deleteLike(Integer filmId, Integer userId) {

        Film film = filmStorage.getFilmById(filmId);
        User user = userStorage.getUserById(userId);
        if (film != null && user != null) {

            if (film.getLikes().contains(userId)) {
                film.getLikes().remove(userId);
            } else {
                throw new ValidationException("Пользователь с id=" + userId + " не ставил лайк");
            }
        } else {
            throw new ValidationException("Фильма или пользователя с указанным id не существует");
        }

    }

    public List<Film> getPopular(Integer count) {

        if (count < 1) {
            throw new ValidationException("Количество фильмов не может быть меньше 1");
        }

        return filmStorage.getAll().stream()
                .sorted((film1, film2) -> film2.getLikes().size() - film1.getLikes().size())
                .limit(count)
                .collect(Collectors.toList());
    }
}