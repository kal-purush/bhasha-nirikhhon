package ru.yandex.practicum.filmorate.controller;

import org.junit.jupiter.api.Test;
import ru.yandex.practicum.filmorate.exception.ValidationException;
import ru.yandex.practicum.filmorate.model.Film;
import java.time.LocalDate;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class FilmControllerTest {

    FilmController controller = new FilmController();

    @Test
    void createValidFilms() {
        Film film1 = new Film(0, "Rocky", "Sports drama",
                LocalDate.of(1976, 11, 21), 119);
        Film film2 = new Film(0, "In Bruges", "Black comedy-drama crime thriller film",
                LocalDate.of(2008, 1, 17), 107);

        controller.create(film1);
        controller.create(film2);

        assertEquals(2, controller.readAll().size(), "Контроллер не создал фильмы");
        assertEquals(1, (int) film1.getId());
        assertEquals(2, (int) film2.getId());
    }

    @Test
    void createEmptyNameFilm() {
        Film film1 = new Film(0, "", "Sports drama",
                LocalDate.of(1976, 11, 21), 119);

        assertThrows(ValidationException.class, () -> controller.create(film1));
    }

    @Test
    void createWrongDescriptionFilm() {
        Film film2 = new Film(0, "In Bruges", String.format("%201c", ' '),
                LocalDate.of(2008, 1, 17), 107);

        assertThrows(ValidationException.class, () -> controller.create(film2));
    }

    @Test
    void createWrongReleaseDateFilm() {
        Film film1 = new Film(0, "Rocky", "Sports drama",
                LocalDate.of(1895, 12, 27), 119);

        assertThrows(ValidationException.class, () -> controller.create(film1));
    }

    @Test
    void createWrongDurationFilm() {
        Film film2 = new Film(0, "In Bruges", "Black comedy-drama crime thriller film",
                LocalDate.of(2008, 1, 17), -107);

        assertThrows(ValidationException.class, () -> controller.create(film2));
    }

    @Test
    void updateValidFilms() {
        List<Film> listFilm;
        Film film1 = new Film(0, "Rocky", "Sports drama",
                LocalDate.of(1976, 11, 21), 119);
        Film film2 = new Film(0, "In Bruges", "Black comedy-drama crime thriller film",
                LocalDate.of(2008, 1, 17), 107);

        controller.create(film1);
        controller.create(film2);
        listFilm = controller.readAll();

        assertTrue(listFilm.contains(film1));
        assertTrue(listFilm.contains(film1));

        Film updatedFilm1 = new Film(1, "Rocky", "Sports comedy",
                LocalDate.of(1976, 11, 21), 119);
        Film updatedFilm2 = new Film(2, "In Bruges", "Crime thriller film",
                LocalDate.of(2008, 1, 17), 107);

        controller.update(updatedFilm1);
        controller.update(updatedFilm2);
        listFilm = controller.readAll();

        assertTrue(listFilm.contains(updatedFilm1));
        assertTrue(listFilm.contains(updatedFilm2));
        assertFalse(listFilm.contains(film1));
        assertFalse(listFilm.contains(film2));
    }

    @Test
    void updateWrongFilms() {
        Film film1 = new Film(0, "Rocky", "Sports drama",
                LocalDate.of(1976, 11, 21), 119);
        Film film2 = new Film(0, "In Bruges", "Black comedy-drama crime thriller film",
                LocalDate.of(2008, 1, 17), 107);

        controller.create(film1);
        controller.create(film2);

        Film wrongIdFilm1 = new Film(12, "Rocky", "Sports drama",
                LocalDate.of(1976, 11, 21), 119);
        Film wrongIdFilm2 = new Film(0, "In Bruges", "Black comedy-drama crime thriller film",
                LocalDate.of(2008, 1, 17), 107);

        assertThrows(ValidationException.class, () -> controller.update(wrongIdFilm1));
        assertThrows(ValidationException.class, () -> controller.update(wrongIdFilm2));
    }

    @Test
    void readAll() {
        Film film1 = new Film(0, "Rocky", "Sports drama",
                LocalDate.of(1976, 11, 21), 119);
        Film film2 = new Film(0, "In Bruges", "Black comedy-drama crime thriller film",
                LocalDate.of(2008, 1, 17), 107);

        controller.create(film1);
        controller.create(film2);
        List<Film> listFilm = controller.readAll();

        assertEquals(2, listFilm.size());
        assertTrue(listFilm.contains(film1));
        assertTrue(listFilm.contains(film2));
    }
}