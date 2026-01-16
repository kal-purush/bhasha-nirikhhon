package ru.yandex.practicum.filmorate;
import org.junit.Test;
import ru.yandex.practicum.filmorate.controllers.FilmController;
import ru.yandex.practicum.filmorate.exceptions.ValidationException;
import ru.yandex.practicum.filmorate.model.Film;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;

public class FilmControllerTest  {
    private final static FilmController filmController = new FilmController();

    @Test
    public void shouldValidateFilm() {
        Film film = new Film(1, "name", "horror", LocalDate.now(), 20);
        assertTrue(filmController.validation(film));
    }

    @Test(expected = ValidationException.class)
    public void shouldNotValidateFilmDescription() {
        Film film = new Film(1, "name", "horrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorror" +
                "horrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorror" +
                "horrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorrorhorror"
                , LocalDate.of(1654,01,03), 20);
        filmController.validation(film);
    }

    @Test(expected = ValidationException.class)
    public void shouldNotValidateFilmReleaseDate() {
        Film film = new Film(1, "name", "horror", LocalDate.of(1654,01,03), 20);
        filmController.validation(film);
    }

}