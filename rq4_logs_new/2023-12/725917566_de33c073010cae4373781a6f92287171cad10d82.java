package ru.yandex.practicum.filmorate.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.filmorate.exception.FilmNotFoundException;
import ru.yandex.practicum.filmorate.exception.ValidationException;
import ru.yandex.practicum.filmorate.model.Film;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class InMemoryFilmStorage implements FilmStorage {

    private final Map<Integer, Film> films = new HashMap<>();

    private static int filmIdGen = 0;

    @Override
    public Film update(Film film) {

        if (!films.containsKey(film.getId())) {
            throw new ValidationException("Фильм с таким id не найден");
        }

        if (isValidFilm(film)) {
            films.put(film.getId(), film);
            log.info("Фильм с id={} обновлен", film.getId());
        }

        return film;
    }

    @Override
    public Film create(Film film) {


        if (films.containsKey(film.getId())) {
            throw new ValidationException("Фильм с таким id уже создан");
        }

        if (isValidFilm(film)) {

            int currId = ++filmIdGen;
            film.setId(currId);
            films.put(currId, film);
            log.info("Новый фильм с названием: " + film.getName() + " создан");
        }
        return film;
    }

    @Override
    public Film delete(Integer id) {

        Film filmToDelete = films.remove(id);

        if(filmToDelete == null) {
            throw new ValidationException("Фильма с таким id не существует");
        }

        return filmToDelete;

    }

    @Override
    public Film getFilmById(Integer filmId) {

        Film film = films.get(filmId);
        if (film == null) {
            throw new FilmNotFoundException("Фильм с id=" + filmId + " не найден");
        }
        return film;
    }

    @Override
    public List<Film> getAll() {

        return new ArrayList<>(films.values());
    }

    private boolean isValidFilm(Film film) {
        if (film.getName().isEmpty()) {
            throw new ValidationException("Название фильма не должно быть пустым");
        }
        if ((film.getDescription().length()) > 200 || (film.getDescription().isEmpty())) {
            throw new ValidationException("Описание фильма больше 200 символов или пустое ");
        }
        if (film.getReleaseDate().isBefore(LocalDate.of(1895, 12, 28))) {
            throw new ValidationException("Некорректная дата релиза фильма: " + film.getReleaseDate());
        }
        if (film.getDuration() <= 0) {
            throw new ValidationException("Продолжительность должна быть положительной");
        }
        return true;
    }
}