package ru.yandex.practicum.filmorate.service;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.filmorate.exception.NotFoundException;
import ru.yandex.practicum.filmorate.model.Film;
import ru.yandex.practicum.filmorate.model.User;
import ru.yandex.practicum.filmorate.storage.FilmStorage;
import ru.yandex.practicum.filmorate.storage.UserStorage;

import java.util.Collection;
import java.util.Comparator;

@Service
@AllArgsConstructor
public class FilmService {
    private final FilmStorage filmStorage;
    private final UserStorage userStorage;

    public void addLike(Film film, User user) {
        userExistsCheck(user);
        filmExistsCheck(film);
        film.addLike(user);
    }

    public void removeLike(Film film, User user) {
        userExistsCheck(user);
        filmExistsCheck(film);
        film.removeLike(user);
    }

    public Collection<Film> getMostPopularFilms() {
        int filmsSelectionLength = 10;
        return filmStorage.getAllFilms().stream()
                .sorted(Comparator.comparing(Film::getLikesCount).reversed())
                .limit(filmsSelectionLength)
                .toList();
    }

    private void userExistsCheck(User user) {
        if (userStorage.findUser(user.getId()).isEmpty()) {
            throw new NotFoundException(String.format("User with id %d not found", user.getId()));
        }
    }

    private void filmExistsCheck(Film film) {
        if (filmStorage.findFilm(film.getId()).isEmpty()) {
            throw new NotFoundException(String.format("Film with id %d not found", film.getId()));
        }
    }
}