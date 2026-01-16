package ru.yandex.practicum.filmorate.dao.impl;

import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import ru.yandex.practicum.filmorate.dao.FilmDbStorage;
import ru.yandex.practicum.filmorate.dao.UserDbStorage;
import ru.yandex.practicum.filmorate.exeption.EntityNotFoundException;
import ru.yandex.practicum.filmorate.model.Film;
import ru.yandex.practicum.filmorate.model.Mpa;
import ru.yandex.practicum.filmorate.model.User;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@JdbcTest
@RequiredArgsConstructor(onConstructor_ = @Autowired)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FilmDbStorageImplTest {
    private final JdbcTemplate jdbcTemplate;
    private final UserDbStorageImpl userDbStorage;
    private final FriendsUserDBImpl friendsUserDB;


    @Test
    void findAllTest() {
        Film filmOne = new Film(1, "filmOne", "testDescription", LocalDate.of(2000, 12, 20), 167);
        Film filmTwo = new Film(2, "filmTwo", "testDescription", LocalDate.of(2000, 12, 20), 167);
        Mpa mpaOne = new Mpa(1, "G");
        Mpa mpaTwo = new Mpa(2, "PG");
        filmOne.setMpa(mpaOne);
        filmTwo.setMpa(mpaTwo);
        FilmDbStorage filmDbStorage = new FilmDbStorageImpl(jdbcTemplate, userDbStorage);
        filmDbStorage.post(filmOne);
        filmDbStorage.post(filmTwo);
        List<Film> filmList = filmDbStorage.findAll();
        assertFalse(filmList.isEmpty());
        assertTrue(filmList.size() == 2);
        assertEquals(filmList.get(0), filmOne);
        assertEquals(filmList.get(1), filmTwo);
    }

    @Test
    void findFimByIdTest() {
        Film filmOne = new Film(1, "filmOne", "testDescription", LocalDate.of(2000, 12, 20), 167);
        Mpa mpaOne = new Mpa(1, "G");
        filmOne.setMpa(mpaOne);
        FilmDbStorage filmDbStorage = new FilmDbStorageImpl(jdbcTemplate, userDbStorage);
        filmDbStorage.post(filmOne);
        Film film = filmDbStorage.findFimById(1);
        assertNotNull(film);
        assertEquals(film, filmOne);
        assertThrows(EntityNotFoundException.class, () -> filmDbStorage.findFimById(9999));
    }

    @Test
    void postTest() {
        Film filmOne = new Film(1, "filmOne", "testDescription", LocalDate.of(2000, 12, 20), 167);
        Mpa mpaOne = new Mpa(1, "G");
        filmOne.setMpa(mpaOne);
        FilmDbStorage filmDbStorage = new FilmDbStorageImpl(jdbcTemplate, userDbStorage);
        filmDbStorage.post(filmOne);
        Film film = filmDbStorage.findFimById(1);
        assertEquals(film, filmOne);
        assertEquals(film.getName(), filmOne.getName());

    }

    @Test
    void putTest() {
        Film filmOne = new Film(1, "filmOne", "testDescription", LocalDate.of(2000, 12, 20), 167);
        Film filmTwo = new Film(1, "filmTwo", "testDescription", LocalDate.of(2000, 12, 20), 167);
        Mpa mpaOne = new Mpa(1, "G");
        Mpa mpaTwo = new Mpa(2, "PG");
        filmOne.setMpa(mpaOne);
        filmTwo.setMpa(mpaTwo);
        FilmDbStorage filmDbStorage = new FilmDbStorageImpl(jdbcTemplate, userDbStorage);
        filmDbStorage.post(filmTwo);
        Film film = filmDbStorage.findFimById(1);
        assertEquals(film, filmTwo);
    }

    @Test
    void popularTest() {
        Film filmOne = new Film(1, "filmOne", "testDescription", LocalDate.of(2000, 12, 20), 167);
        Film filmTwo = new Film(2, "filmTwo", "testDescription", LocalDate.of(2000, 12, 20), 167);
        Film filmThree = new Film(3, "filmTwo", "testDescription", LocalDate.of(2000, 12, 20), 167);
        Mpa mpaOne = new Mpa(1, "G");
        Mpa mpaTwo = new Mpa(2, "PG");
        Mpa mpaThree = new Mpa(3, "G");
        FilmDbStorage filmDbStorage = new FilmDbStorageImpl(jdbcTemplate, userDbStorage);
        filmOne.setMpa(mpaOne);
        filmTwo.setMpa(mpaTwo);
        filmThree.setMpa(mpaThree);
        filmOne.setRate(5);
        filmTwo.setRate(10);
        filmThree.setRate(2);
        filmDbStorage.post(filmOne);
        filmDbStorage.post(filmTwo);
        filmDbStorage.post(filmThree);
        List<Film> filmList = filmDbStorage.popular(10);
        assertTrue(filmList.size() == 3);
        assertEquals(filmList.get(0), filmTwo);
        assertEquals(filmList.get(1), filmOne);
        assertEquals(filmList.get(2), filmThree);
        List<Film> filmListTwo = filmDbStorage.popular(2);
        assertTrue(filmListTwo.size() == 2);
        assertEquals(filmListTwo.get(0), filmTwo);
        assertEquals(filmListTwo.get(1), filmOne);
    }

    @Test
    void addLikeTest() {
        Film filmOne = new Film(1, "filmOne", "testDescription", LocalDate.of(2000, 12, 20), 167);
        Mpa mpaOne = new Mpa(1, "G");
        filmOne.setMpa(mpaOne);
        filmOne.setRate(20);
        FilmDbStorage filmDbStorage = new FilmDbStorageImpl(jdbcTemplate, userDbStorage);
        filmDbStorage.post(filmOne);
        User newUser = new User(1, "user@email.ru", "vanya123", "Ivan Petrov", LocalDate.of(1990, 1, 1));
        UserDbStorage userStorage = new UserDbStorageImpl(jdbcTemplate, friendsUserDB);
        userStorage.post(newUser);
        Film filmBeforeLike = filmDbStorage.findFimById(1);
        assertTrue(filmBeforeLike.getRate() == 20);
        filmDbStorage.addLike(1, 1);
        Film filmAfterLike = filmDbStorage.findFimById(1);
        assertTrue(filmAfterLike.getRate() == 21);
    }

    @Test
    void dellLikeTest() {
        Film filmOne = new Film(1, "filmOne", "testDescription", LocalDate.of(2000, 12, 20), 167);
        Mpa mpaOne = new Mpa(1, "G");
        filmOne.setMpa(mpaOne);
        filmOne.setRate(20);
        FilmDbStorage filmDbStorage = new FilmDbStorageImpl(jdbcTemplate, userDbStorage);
        filmDbStorage.post(filmOne);
        User newUser = new User(1, "user@email.ru", "vanya123", "Ivan Petrov", LocalDate.of(1990, 1, 1));
        UserDbStorage userStorage = new UserDbStorageImpl(jdbcTemplate, friendsUserDB);
        userStorage.post(newUser);
        Film filmBeforeLike = filmDbStorage.findFimById(1);
        assertTrue(filmBeforeLike.getRate() == 20);
        filmDbStorage.dellLike(1, 1);
        Film filmAfterLike = filmDbStorage.findFimById(1);
        assertTrue(filmAfterLike.getRate() == 19);
    }
}