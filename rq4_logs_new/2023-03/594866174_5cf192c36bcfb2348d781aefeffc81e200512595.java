package ru.yandex.practicum.filmorate.storage.film;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.filmorate.model.Film;
import ru.yandex.practicum.filmorate.storage.user.UserStorage;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@ConditionalOnProperty(name = "app.storage.type", havingValue = "memory")
public class InMemoryFilmAudienceStorage implements FilmAudienceStorage {
    public static final Comparator<Film> COMPARATOR = Comparator.comparing(Film::getRate).reversed();
    private FilmStorage storage;
    private UserStorage userStorage;

    @Autowired
    public InMemoryFilmAudienceStorage (FilmStorage storage, UserStorage userStorage) {
        this.storage = storage;
        this.userStorage = userStorage;
    }

    @Override
    public void addLike(long filmId, long userId) {
        Film film = storage.findObjectById(filmId);
        film.addLike(userId);
    }

    @Override
    public void removeLike(long filmId, long userId) {
        Film film = storage.findObjectById(filmId);
        userStorage.findObjectById(userId);
        film.removeLike(userId);
    }

    @Override
    public List<Film> getPopularFilmsList(int count) {
        return storage.getAllObjects().stream()
                .sorted(COMPARATOR)
                .limit(count)
                .collect(Collectors.toList());
    }
}